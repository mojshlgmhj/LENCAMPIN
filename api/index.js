const functions = require("firebase-functions");
const admin = require("firebase-admin");
const fetch = require("node-fetch");

admin.initializeApp();

const db = admin.firestore();

// Helper function to delay execution
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

exports.processCampaignQueue = functions.runWith({ timeoutSeconds: 540, memory: '1GB' }).firestore
  .document("artifacts/{appId}/public/data/campaigns/{campaignId}")
  .onCreate(async (snap, context) => {
    const campaignId = context.params.campaignId;
    const campaignRef = snap.ref;
    const campaignData = snap.data();

    if (!campaignData || campaignData.status !== "pending") {
      functions.logger.log(`Campaign ${campaignId} is not pending. Aborting.`);
      return null;
    }

    try {
      await campaignRef.update({
        status: "in-progress",
        lastProcessed: admin.firestore.FieldValue.serverTimestamp(),
      });
      functions.logger.log(`Campaign ${campaignId} started processing.`);

      const { audience, message, imageUrl, delay: sendDelay, pageId, accessToken } = campaignData;
      
      for (const recipientId of audience) {
        // More reliable way to check status before each send
        const currentDoc = await campaignRef.get();
        let currentStatus = currentDoc.data().status;

        // Loop to wait if paused
        while (currentStatus === "paused") {
          functions.logger.log(`Campaign ${campaignId} is paused. Waiting...`);
          await delay(5000); // Wait 5 seconds before checking again
          const refreshedDoc = await campaignRef.get();
          currentStatus = refreshedDoc.data().status;
        }

        if (currentStatus === "stopped") {
          functions.logger.log(`Campaign ${campaignId} was stopped. Halting.`);
          break; // Exit the loop
        }
        
        const url = `https://graph.facebook.com/v20.0/me/messages?access_token=${accessToken}`;
        let messageData = {};
        if (message) messageData.text = message;
        if (imageUrl) messageData.attachment = { type: "image", payload: { url: imageUrl, is_reusable: true } };
        
        const payload = {
          messaging_type: "RESPONSE",
          recipient: { id: recipientId },
          message: messageData,
        };

        let success = false;
        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const responseData = await response.json();
            if (responseData.error) {
                throw new Error(responseData.error.message);
            }
            success = true;
        } catch (error) {
            functions.logger.error(`Failed to send to ${recipientId} for campaign ${campaignId}:`, error);
        }

        if (success) {
            await campaignRef.update({ successCount: admin.firestore.FieldValue.increment(1) });
        } else {
            await campaignRef.update({ failureCount: admin.firestore.FieldValue.increment(1) });
        }
        
        await delay(sendDelay * 1000);
      }

      const finalDoc = await campaignRef.get();
      if (finalDoc.data().status !== "stopped") {
        await campaignRef.update({
          status: "completed",
          lastProcessed: admin.firestore.FieldValue.serverTimestamp(),
        });
        functions.logger.log(`Campaign ${campaignId} completed successfully.`);
      }

    } catch (error) {
      functions.logger.error(`Error processing campaign ${campaignId}:`, error);
      await campaignRef.update({
        status: "failed",
        error: error.message,
      });
    }
    return null;
  });





