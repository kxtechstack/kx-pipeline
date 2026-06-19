//queuemngr.js
const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// Sort articles by publishedDate - newest first
const sortByNewest = (articles) => {
  return articles.sort((a, b) => {

    const dateA = a.publishedDate
      ? new Date(a.publishedDate).getTime()
      : 0;

    const dateB = b.publishedDate
      ? new Date(b.publishedDate).getTime()
      : 0;

    return dateB - dateA;
  });
};

// Push articles into Redis queue, one by one
const pushToQueue = async (articles, jobId) => {
  
  const queueKey = `raw:${jobId}`;

  for (const article of articles) {
    await redis.rpush(queueKey, JSON.stringify(article));
  }

  // Set 24 hour expiry -- this is the ONLY way raw data goes away now.
  // We no longer destructively pop items during processing, so the raw
  // queue stays intact and inspectable until it naturally expires.
  await redis.expire(queueKey, 86400);

  console.log(`Pushed ${articles.length} articles to ${queueKey}`);

  return queueKey;
};

// Get how many items are in the queue (for status reporting only)
const getQueueLength = async (queueKey) => {
  return await redis.llen(queueKey);
};

/**
 * Read articles from the raw queue WITHOUT removing them (non-destructive).
 * Uses lrange instead of lpop, so the raw data stays in Redis for its full
 * 24-hour TTL even after the pipeline finishes processing -- this means
 * if anything crashes mid-pipeline, the raw fetch data is still there and
 * we don't need to call Exa again to recover it.
 *
 * @param {string} queueKey
 * @param {number} startIndex - 0-based index to start reading from
 * @param {number} batchSize
 * @returns {Array} batch of articles (parsed from JSON)
 */
const readBatch = async (queueKey, startIndex, batchSize = 10) => {
  const endIndex = startIndex + batchSize - 1;
  const items = await redis.lrange(queueKey, startIndex, endIndex);

  return items.map(item => (typeof item === 'string' ? JSON.parse(item) : item));
};

// Save job status
const setStatus = async (jobId, statusData) => {
  await redis.set(`status:${jobId}`, JSON.stringify(statusData), { ex: 86400 });
};

// Get job status
const getStatus = async (jobId) => {
  const data = await redis.get(`status:${jobId}`);
  if (!data) return null;
  return typeof data === 'string' ? JSON.parse(data) : data;
};

module.exports = {
  redis,
  sortByNewest,
  pushToQueue,
  readBatch,
  getQueueLength,
  setStatus,
  getStatus
};