/**
 * testEdgeCases.js
 * ==================
 * Tests the pipeline against:
 *   1. No data (0 articles)
 *   2. Low data (2-3 articles)
 *   3. Edge cases: missing text, missing publishedDate, very short text,
 *      very long text, special characters
 *
 * Run each test independently so a crash in one doesn't block the others.
 *
 * Usage:
 *   node testEdgeCases.js
 */

require('dotenv').config();
const { removeUrlDuplicates } = require('./modules/deduplicator');
const { removeSameTopicArticles } = require('./modules/topicDedup');
const { filterLowQualityArticles } = require('./modules/qualityFilter');
const { pushToProcessedQueue, getProcessedQueueLength } = require('./modules/processedQueue');
const { processQueueInBatches } = require('./modules/llmRelevanceProcessor');

const TEST_CLIENT_ID = 'a1000000-0000-0000-0000-000000000048';
const TEST_INDUSTRY = 'Banking';
const TEST_JOB_PREFIX = 'edgecase_test';

const runTest = async (label, fn) => {
  console.log('\n' + '='.repeat(70));
  console.log(`TEST: ${label}`);
  console.log('='.repeat(70));
  try {
    await fn();
    console.log(`✅ PASSED (no crash): ${label}`);
  } catch (err) {
    console.log(`❌ CRASHED: ${label}`);
    console.log(`   Error: ${err.message}`);
    console.log(err.stack);
  }
};

// ── Test 1: No data at all ───────────────────────────────────────────────────
const testNoData = async () => {
  const jobId = `${TEST_JOB_PREFIX}_nodata_${Date.now()}`;
  console.log('Running full chain with an empty articles array...');

  const afterUrl = await removeUrlDuplicates([], TEST_CLIENT_ID);
  console.log(`  After URL dedup: ${afterUrl.length}`);

  const afterTopic = await removeSameTopicArticles(afterUrl, TEST_CLIENT_ID);
  console.log(`  After topic dedup: ${afterTopic.length}`);

  const afterQuality = await filterLowQualityArticles(afterTopic);
  console.log(`  After quality filter: ${afterQuality.length}`);

  const queueKey = await pushToProcessedQueue(afterQuality, jobId);
  console.log(`  Pushed to queue: ${queueKey}`);

  const result = await processQueueInBatches(queueKey, TEST_CLIENT_ID, TEST_INDUSTRY, jobId);
  console.log(`  LLM result:`, result);
};

// ── Test 2: Low data (2 articles) ────────────────────────────────────────────
const testLowData = async () => {
  const jobId = `${TEST_JOB_PREFIX}_lowdata_${Date.now()}`;
  const articles = [
    {
      title: 'FDIC Proposes New Capital Requirements for Mid-Size Banks',
      url: `https://test.com/low-data-1-${Date.now()}`,
      text: 'The FDIC today proposed new capital requirements for mid-size banks, requiring institutions with assets over $50 billion to maintain higher liquidity buffers. The rule would take effect in 2027 and affects approximately 40 regional banks across the country.',
      publishedDate: new Date().toISOString(),
    },
    {
      title: 'Treasury Issues Guidance on Cross-Border Payment Reporting',
      url: `https://test.com/low-data-2-${Date.now()}`,
      text: 'The U.S. Treasury Department issued new guidance requiring banks to report cross-border payments over $10,000 within 48 hours, as part of an expanded anti-money laundering framework targeting international wire transfers.',
      publishedDate: new Date().toISOString(),
    },
  ];

  console.log(`Running full chain with ${articles.length} articles...`);

  const afterUrl = await removeUrlDuplicates(articles, TEST_CLIENT_ID);
  console.log(`  After URL dedup: ${afterUrl.length}`);

  const afterTopic = await removeSameTopicArticles(afterUrl, TEST_CLIENT_ID);
  console.log(`  After topic dedup: ${afterTopic.length}`);

  const afterQuality = await filterLowQualityArticles(afterTopic);
  console.log(`  After quality filter: ${afterQuality.length}`);

  const queueKey = await pushToProcessedQueue(afterQuality, jobId);
  console.log(`  Pushed to queue: ${queueKey}`);

  const result = await processQueueInBatches(queueKey, TEST_CLIENT_ID, TEST_INDUSTRY, jobId);
  console.log(`  LLM result:`, result);
};

// ── Test 3: Edge case - missing text field ───────────────────────────────────
const testMissingText = async () => {
  const jobId = `${TEST_JOB_PREFIX}_missingtext_${Date.now()}`;
  const articles = [
    {
      title: 'Article With No Text Field At All',
      url: `https://test.com/missing-text-${Date.now()}`,
      publishedDate: new Date().toISOString(),
      // text field intentionally omitted
    },
  ];

  const afterUrl = await removeUrlDuplicates(articles, TEST_CLIENT_ID);
  const afterTopic = await removeSameTopicArticles(afterUrl, TEST_CLIENT_ID);
  const afterQuality = await filterLowQualityArticles(afterTopic);
  console.log(`  After quality filter (should be 0, filtered for length): ${afterQuality.length}`);
};

// ── Test 4: Edge case - missing publishedDate ────────────────────────────────
const testMissingDate = async () => {
  const jobId = `${TEST_JOB_PREFIX}_missingdate_${Date.now()}`;
  const articles = [
    {
      title: 'Article With No Published Date',
      url: `https://test.com/missing-date-${Date.now()}`,
      text: 'This article has full content describing a new regulatory requirement for banks, but is deliberately missing the publishedDate field to test how the pipeline handles that gap, which can happen with some real sources.',
      // publishedDate intentionally omitted
    },
  ];

  const afterUrl = await removeUrlDuplicates(articles, TEST_CLIENT_ID);
  const afterTopic = await removeSameTopicArticles(afterUrl, TEST_CLIENT_ID);
  const afterQuality = await filterLowQualityArticles(afterTopic);
  console.log(`  After quality filter (no crash expected): ${afterQuality.length}`);
};

// ── Test 5: Edge case - very short text (paywall simulation) ────────────────
const testVeryShortText = async () => {
  const articles = [
    {
      title: 'Paywalled Article',
      url: `https://test.com/short-text-${Date.now()}`,
      text: 'Subscribe to continue reading.',
      publishedDate: new Date().toISOString(),
    },
  ];

  const afterUrl = await removeUrlDuplicates(articles, TEST_CLIENT_ID);
  const afterTopic = await removeSameTopicArticles(afterUrl, TEST_CLIENT_ID);
  const afterQuality = await filterLowQualityArticles(afterTopic);
  console.log(`  After quality filter (should be 0, too short): ${afterQuality.length}`);
};

// ── Test 6: Edge case - special characters / unicode ─────────────────────────
const testSpecialCharacters = async () => {
  const articles = [
    {
      title: 'Bank Faces €500M Fine — Regulators Cite "严重" Compliance Failures™',
      url: `https://test.com/special-chars-${Date.now()}`,
      text: 'Regulators announced a €500 million fine against a major bank for compliance failures, citing issues with — among other things — anti-money laundering controls. The fine includes special characters: emoji 🏦, currency symbols £€¥$, and quotes "like this" and \'this\'.',
      publishedDate: new Date().toISOString(),
    },
  ];

  const afterUrl = await removeUrlDuplicates(articles, TEST_CLIENT_ID);
  const afterTopic = await removeSameTopicArticles(afterUrl, TEST_CLIENT_ID);
  const afterQuality = await filterLowQualityArticles(afterTopic);
  console.log(`  After quality filter (should pass, special chars handled): ${afterQuality.length}`);
};

const run = async () => {
  await runTest('No data (0 articles)', testNoData);
  await runTest('Low data (2 articles)', testLowData);
  await runTest('Missing text field', testMissingText);
  await runTest('Missing publishedDate field', testMissingDate);
  await runTest('Very short text (paywall simulation)', testVeryShortText);
  await runTest('Special characters / unicode', testSpecialCharacters);

  console.log('\n' + '='.repeat(70));
  console.log('ALL EDGE CASE TESTS COMPLETE');
  console.log('='.repeat(70));
};

run().catch(console.error);