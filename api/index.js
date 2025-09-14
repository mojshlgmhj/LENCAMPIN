/* eslint-disable no-console */
const functions = require("firebase-functions");
const admin = require("firebase-admin");

// في Node 18+ fetch متاح تلقائياً، لو بيئة قديمة استعمل node-fetch
const fetch = global.fetch || require("node-fetch");

admin.initializeApp();
const db = admin.firestore();

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

/** إرسال مع Retry/backoff */
async function sendWithRetry(url, payload, max = 5) {
  let attempt = 0;
  while (true) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok || data.error) {
        const msg = data?.error?.message || `HTTP ${res.status}`;
        // أخطاء قابلة لإعادة المحاولة: 429/5xx
        if ((res.status === 429 || res.status >= 500) && attempt < max) {
          throw new Error(`retryable:${msg}`);
        }
        throw new Error(msg);
      }
      return { ok: true, data };
    } catch (e) {
      attempt++;
      const isRetryable = String(e.message || "").startsWith("retryable:");
      if (!isRetryable || attempt > max) {
        return { ok: false, error: e };
      }
      // backoff مع jitter
      const backoff = Math.min(30000, 500 * 2 ** (attempt - 1));
      const jitter = Math.floor(Math.random() * 500);
      await delay(backoff + jitter);
    }
  }
}

exports.processCampaignQueue = functions
  .runWith({ timeoutSeconds: 540, memory: "1GB" })
  .firestore
  .document("artifacts/{appId}/public/data/campaigns/{campaignId}")
  .onCreate(async (snap, context) => {
    const campaignId = context.params.campaignId;
    const campaignRef = snap.ref;
    const campaign = snap.data() || {};

    if (campaign.status !== "pending") {
      functions.logger.log(`Campaign ${campaignId} not pending. Abort.`);
      return null;
    }

    // تأكيد الحقول الافتراضية
    const safe = (v, d) => (v === undefined || v === null ? d : v);
    const state = {
      audience: Array.isArray(campaign.audience) ? campaign.audience : [],
      message: safe(campaign.message, ""),
      imageUrl: safe(campaign.imageUrl, ""),
      delaySec: Number(safe(campaign.delay, 0)) || 0,
      pageId: campaign.pageId,
      accessToken: campaign.accessToken,
      currentIndex: Number(safe(campaign.currentIndex, 0)) || 0,
      successCount: Number(safe(campaign.successCount, 0)) || 0,
      failureCount: Number(safe(campaign.failureCount, 0)) || 0,
    };

    if (!state.pageId || !state.accessToken) {
      await campaignRef.update({
        status: "failed",
        error: "Missing pageId/accessToken",
      });
      return null;
    }

    // لا نسمح برسالة فارغة بالكامل
    if (!state.message && !state.imageUrl) {
      await campaignRef.update({
        status: "failed",
        error: "message or imageUrl required",
      });
      return null;
    }

    // ضع الحالة in-progress
    await campaignRef.update({
      status: "in-progress",
      lastProcessed: admin.firestore.FieldValue.serverTimestamp(),
      currentIndex: state.currentIndex,
      successCount: state.successCount,
      failureCount: state.failureCount,
    });
    functions.logger.log(`Campaign ${campaignId} started at index ${state.currentIndex}`);

    const url = `https://graph.facebook.com/v20.0/me/messages?access_token=${encodeURIComponent(state.accessToken)}`;

    try {
      for (let i = state.currentIndex; i < state.audience.length; i++) {
        // اقرأ الحالة الحالية قبل كل إرسال
        const fresh = (await campaignRef.get()).data();
        const status = fresh?.status || "stopped";

        if (status === "paused") {
          functions.logger.log(`Campaign ${campaignId} paused at ${i}. Waiting...`);
          // انتظر حتى تتغير الحالة
          while (true) {
            await delay(5000);
            const again = (await campaignRef.get()).data();
            if (!again) break;
            if (again.status === "in-progress") break; // تم الاستئناف
            if (again.status === "stopped" || again.status === "failed") {
              functions.logger.log(`Campaign ${campaignId} halted while paused.`);
              return null;
            }
          }
        }
        if (status === "stopped" || status === "failed") {
          functions.logger.log(`Campaign ${campaignId} stopped at ${i}.`);
          return null;
        }

        const recipientId = state.audience[i];
        const messageData = {};
        if (state.message) messageData.text = state.message;
        if (state.imageUrl) {
          messageData.attachment = {
            type: "image",
            payload: { url: state.imageUrl, is_reusable: true },
          };
        }

        const payload = {
          messaging_type: "RESPONSE",
          recipient: { id: recipientId },
          message: messageData,
        };

        const result = await sendWithRetry(url, payload, 5);

        // حدث العدادات والفهرس بشكل ذري
        await db.runTransaction(async (tx) => {
          const doc = await tx.get(campaignRef);
          const d = doc.data() || {};
          const incField = result.ok ? "successCount" : "failureCount";
          tx.update(campaignRef, {
            [incField]: admin.firestore.FieldValue.increment(1),
            currentIndex: i + 1,
            lastProcessed: admin.firestore.FieldValue.serverTimestamp(),
            lastRecipient: recipientId,
            lastError: result.ok ? admin.firestore.FieldValue.delete() : String(result.error),
          });
        });

        if (!result.ok) {
          functions.logger.error(`Send failed to ${recipientId} (campaign ${campaignId}): ${result.error}`);
        }

        if (state.delaySec > 0) {
          await delay(state.delaySec * 1000);
        }
      }

      // اكتمال لو لم يتم إيقاف الحملة
      const finalDoc = (await campaignRef.get()).data();
      if (finalDoc?.status !== "stopped" && finalDoc?.status !== "failed") {
        await campaignRef.update({
          status: "completed",
          lastProcessed: admin.firestore.FieldValue.serverTimestamp(),
        });
        functions.logger.log(`Campaign ${campaignId} completed successfully.`);
      }
    } catch (err) {
      functions.logger.error(`Error processing campaign ${campaignId}:`, err);
      await campaignRef.update({
        status: "failed",
        error: String(err.message || err),
      });
    }

    return null;
  });
