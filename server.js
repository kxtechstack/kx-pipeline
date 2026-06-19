require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');

const { fetchFromExa } = require('./modules/fetcher');
const { sortByNewest, pushToQueue, readBatch, getQueueLength, setStatus, getStatus } = require('./modules/queueManager');
const { removeUrlDuplicates } = require('./modules/deduplicator');
const { removeSameTopicArticles } = require('./modules/topicDedup');
const { filterLowQualityArticles } = require('./modules/qualityFilter');
const { pushToProcessedQueue } = require('./modules/processedQueue');
const { startJobTracking, updateJobStage, completeJobTracking, markFullyCompleted, failJobTracking } = require('./modules/jobStatusTracker');
const { processQueueInBatches } = require('./modules/llmRelevanceProcessor');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));


// Main pipeline trigger
// Requires clientId, promptText, AND industry from the very first call --
// industry flows all the way through to the LLM relevance step at the end.
app.post('/run', async (req, res) => {

  const { clientId, promptText, industry } = req.body;

  if (!clientId || !promptText || !industry) {
    return res.status(400).json({ error: 'clientId, promptText, and industry are all required' });
  }

  // Generate a unique job ID
  const jobId = `job_${Date.now()}`;

  // Send response immediately - processing happens in background
  res.json({ jobId, status: 'started' });

  // Run the pipeline in background (don't block response)
  runPipeline(jobId, clientId, promptText, industry);
});

// Status check route
app.get('/status/:jobId', async (req, res) => {
  const status = await getStatus(req.params.jobId);
  if (!status) {
    return res.status(404).json({ error: 'Job not found' });
  }
  res.json(status);
});


// ============================================
// MAIN PIPELINE FUNCTION
// ============================================

const runPipeline = async (jobId, clientId, promptText, industry) => {

  try {

    await startJobTracking(jobId, clientId, promptText);
    await setStatus(jobId, { status: 'fetching', message: 'Calling Exa API...' });

    // Step 1 - Fetch from Exa
    const articles = await fetchFromExa(promptText);
    console.log("\n========== PROMPT SENT TO EXA ==========\n");
    console.log(promptText);
    console.log("Industry:", industry);

    await updateJobStage(jobId, 'fetching', { fetched: articles.length });
    await setStatus(jobId, { status: 'sorting', total: articles.length, message: `Fetched ${articles.length} articles` });

    // Step 2 - Sort newest first
    const sorted = sortByNewest(articles);
    console.log("\n========== FIRST 20 SORTED ARTICLES ==========\n");

    sorted.slice(0, 90).forEach((article, index) => {
      console.log(
        `${index + 1}. ${article.publishedDate} | ${article.title}`
      );
    });

    // Step 3 - Push to Redis RAW queue (temporary holding for this run only)
    const queueKey = await pushToQueue(sorted, jobId);

    const queueLength = await getQueueLength(queueKey);

    console.log("Raw Queue Key:", queueKey);
    console.log("Raw Queue Length:", queueLength);

    await updateJobStage(jobId, 'queued', { rawQueueKey: queueKey });
    await setStatus(jobId, { status: 'queued', total: sorted.length, queueKey, message: 'Pushed to Redis raw queue' });

    // Step 4 - Read all articles from raw queue for URL dedup (batches of 10)
    let allCleanArticles = [];
    let processedCount = 0;

    const totalRawCount = await getQueueLength(queueKey);
    let startIndex = 0;

    while (startIndex < totalRawCount) {

      const batch = await readBatch(queueKey, startIndex, 10);

      const afterUrlCheck = await removeUrlDuplicates(batch, clientId);

      allCleanArticles.push(...afterUrlCheck);
      processedCount += batch.length;
      startIndex += 10;

      await setStatus(jobId, {
        status: 'deduplicating',
        total: sorted.length,
        processed: processedCount,
        remaining: Math.max(totalRawCount - processedCount, 0),
        message: `Processed ${processedCount}/${sorted.length} for URL duplicates`
      });
    }

    // Level 2 - Embedding-based topic dedup check
    await updateJobStage(jobId, 'url_dedup', { afterUrlCheck: allCleanArticles.length });
    await setStatus(jobId, {
      status: 'topic_dedup',
      total: sorted.length,
      message: 'Running embedding-based topic dedup check...'
    });

    const finalArticles = await removeSameTopicArticles(allCleanArticles, clientId);

    // Level 3 - Quality filter
    await updateJobStage(jobId, 'topic_dedup', { afterTopicDedup: finalArticles.length });
    await setStatus(jobId, {
      status: 'quality_filter',
      total: sorted.length,
      message: 'Running quality filter (length, language, freshness)...'
    });

    const qualityCheckedArticles = await filterLowQualityArticles(finalArticles);

    // Step 5 - Push final clean articles into the PROCESSED queue
    const processedQueueKey = await pushToProcessedQueue(qualityCheckedArticles, jobId);

    await updateJobStage(jobId, 'pushed_to_processed', {
      afterQualityFilter: qualityCheckedArticles.length,
      pushedToQueue: qualityCheckedArticles.length,
      processedQueueKey,
    });

    await setStatus(jobId, {
      status: 'llm_processing',
      total: sorted.length,
      processedQueueKey,
      message: `Running LLM relevance classification on ${qualityCheckedArticles.length} articles for industry: ${industry}...`
    });

    // Step 6 - LLM relevance classification, industry flows through here too
    const llmResult = await processQueueInBatches(processedQueueKey, clientId, industry, jobId);

    await updateJobStage(jobId, 'llm_processing', {
      afterLlm: llmResult.relevant,
      storedFinal: llmResult.relevant,
    });

    // DONE - full pipeline including LLM is now complete
    await setStatus(jobId, {
      status: 'completed',
      total: sorted.length,
      afterUrlCheck: allCleanArticles.length,
      afterTopicDedup: finalArticles.length,
      afterQualityFilter: qualityCheckedArticles.length,
      afterLlmRelevant: llmResult.relevant,
      afterLlmIrrelevant: llmResult.irrelevant,
      message: `Done! ${llmResult.relevant} relevant articles stored in Qdrant + Supabase, ${llmResult.irrelevant} marked irrelevant.`
    });

    await markFullyCompleted(jobId);

    console.log('Pipeline completed:', jobId);
    console.log(`Total: ${sorted.length}, After URL check: ${allCleanArticles.length}, After Topic Dedup: ${finalArticles.length}, After Quality Filter: ${qualityCheckedArticles.length}`);
    console.log(`LLM Relevant: ${llmResult.relevant}, LLM Irrelevant: ${llmResult.irrelevant}`);

  } catch (error) {
    console.error('Pipeline error:', error);
    await setStatus(jobId, { status: 'failed', error: error.message });
    await failJobTracking(jobId, 'unknown', error.message);
  }
};


const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`KX Pipeline server running on port ${PORT}`);
});

setInterval(() => {
  console.log("alive...");
}, 10000);