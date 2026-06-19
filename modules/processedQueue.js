/**
 * processedQueue.js
 * ===================
 * After articles survive URL dedup, topic dedup, and quality filtering,
 * they get pushed into a SEPARATE Redis queue from the raw fetch queue.
 *
 * Raw queue   : raw:<jobId>        -- temporary holding for newly fetched
 *                                     articles, emptied during dedup/filter
 * Processed Q : processed:<jobId> -- final clean articles, ready to be
 *                                     pulled in batches for LLM relevance
 *                                     classification (tomorrow's work)
 *
 * Kept as its own module (separate from queueManager.js) so the raw-fetch
 * queue logic and the post-filter queue logic don't get tangled together.
 */

const { redis } = require('./queueManager');

const PROCESSED_QUEUE_EXPIRY_SECONDS = 86400; // 24 hours, same as raw queue

// Push the final, fully-filtered articles into the processed queue
const pushToProcessedQueue = async (articles, jobId) => {
  const queueKey = `processed:${jobId}`;

  if (!articles || articles.length === 0) {
    console.log(`[ProcessedQueue] No articles to push for ${queueKey} (0 survived filtering)`);
    return queueKey;
  }

  for (const article of articles) {
    await redis.rpush(queueKey, JSON.stringify(article));
  }

  await redis.expire(queueKey, PROCESSED_QUEUE_EXPIRY_SECONDS);

  console.log(`[ProcessedQueue] Pushed ${articles.length} clean articles to ${queueKey}`);
  return queueKey;
};

// Check how many articles are waiting in the processed queue
const getProcessedQueueLength = async (queueKey) => {
  return await redis.llen(queueKey);
};

// Pull a batch from the processed queue (this is what tomorrow's LLM step will use)
const pullProcessedBatch = async (queueKey, batchSize = 10) => {
  const batch = [];
  for (let i = 0; i < batchSize; i++) {
    const item = await redis.lpop(queueKey);
    if (!item) break;
    const article = typeof item === 'string' ? JSON.parse(item) : item;
    batch.push(article);
  }
  return batch;
};

module.exports = {
  pushToProcessedQueue,
  getProcessedQueueLength,
  pullProcessedBatch,
};