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
const { generateHighlight } = require('./modules/highlightGenerator');
const { createClient } = require('@supabase/supabase-js');
const { QdrantClient } = require('@qdrant/js-client-rest');
const { askQuestion } = require('./modules/ragChat');

const supabaseClient = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const qdrantClient = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});
const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));


app.post('/ask', async (req, res) => {
  try {
    const { question, clientId, industry } = req.body;
    if (!question || !clientId || !industry) {
      return res.status(400).json({ error: 'question, clientId, and industry are required' });
    }
    const result = await askQuestion(question, clientId, industry);
    return res.json(result);
  } catch (err) {
    console.error('[Ask] Error:', err.message);
    return res.status(500).json({ error: err.message });
  }
});

// Main pipeline trigger
app.post('/run', async (req, res) => {
  const { clientId, promptText, industry } = req.body;

  if (!clientId || !promptText || !industry) {
    return res.status(400).json({ error: 'clientId, promptText, and industry are all required' });
  }

  const jobId = `job_${Date.now()}`;
  res.json({ jobId, status: 'started' });
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

// Similar articles route -- uses Qdrant recommend API via point ID
// (recommend works without needing to manually extract the vector,
// which the JS client doesn't expose cleanly from scroll results)
app.get('/similar/:signalId', async (req, res) => {
  try {
    const { signalId } = req.params;
    const TOP_SIMILAR = 3;

    // Step 1 — get signal from Supabase
    const { data: signal, error } = await supabaseClient
      .from('policy_signals')
      .select('article_id, client_id, industry, signal_title, source_article_url')
      .eq('id', signalId)
      .single();

    if (error || !signal) {
      return res.status(404).json({ error: 'Signal not found' });
    }

    if (!signal.article_id) {
      return res.status(200).json({ similar: [], reason: 'No article_id on this signal' });
    }

    // Step 2 — find this article's first chunk point ID in Qdrant
    const chunksResult = await qdrantClient.scroll(POLICY_COLLECTION, {
      filter: {
        must: [{ key: 'article_id', match: { value: signal.article_id } }],
      },
      limit: 1,
      with_vectors: false,
      with_payload: false,
    });

    if (!chunksResult.points || chunksResult.points.length === 0) {
      return res.status(200).json({ similar: [], reason: 'No chunks found in Qdrant for this article' });
    }

    const pointId = chunksResult.points[0].id;

    // Step 3 — recommend API finds similar points from other articles
    const recommended = await qdrantClient.recommend(POLICY_COLLECTION, {
      positive: [pointId],
      limit: 20,
      with_payload: true,
      filter: {
        must: [
          { key: 'client_id', match: { value: signal.client_id } },
          { key: 'industry', match: { value: signal.industry } },
        ],
      },
    });

    // Step 4 — deduplicate by article_id, exclude same article, return top 3
    const seen = new Set();
    const similar = [];

    for (const point of recommended) {
      const articleId = point.payload.article_id;
      const title = point.payload.title;
      if (articleId === signal.article_id) continue;
      if (seen.has(articleId || title)) continue;
      seen.add(articleId || title);
      similar.push({
          article_id: articleId,
          title,
          url: point.payload.url,
          score: point.score,
        });
      if (similar.length >= TOP_SIMILAR) break;
    }

   // Fetch signal_title and signal id from policy_signals for each result
    const articleIds = similar.map(s => s.article_id);
    const { data: signals } = await supabaseClient
      .from('policy_signals')
      .select('id, signal_title, article_id')
      .in('article_id', articleIds)
      .eq('client_id', signal.client_id);

    const signalMap = {};
    if (signals) {
      signals.forEach(s => { signalMap[s.article_id] = s; });
    }

    const enriched = similar.map(s => ({
      signal_id: signalMap[s.article_id]?.id || null,
      title: signalMap[s.article_id]?.signal_title || s.title,
      url: s.url,
      score: s.score,
    }));

    return res.json({ similar: enriched });

  } catch (err) {
    console.error('[Similar] Error:', err.message);
    return res.status(500).json({ error: err.message });
  }
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
      console.log(`${index + 1}. ${article.publishedDate} | ${article.title}`);
    });

    // Step 3 - Push to Redis RAW queue
    const queueKey = await pushToQueue(sorted, jobId);
    const queueLength = await getQueueLength(queueKey);

    console.log("Raw Queue Key:", queueKey);
    console.log("Raw Queue Length:", queueLength);

    await updateJobStage(jobId, 'queued', { rawQueueKey: queueKey });
    await setStatus(jobId, { status: 'queued', total: sorted.length, queueKey, message: 'Pushed to Redis raw queue' });

    // Step 4 - URL dedup in batches of 10
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

    // Level 2 - Topic dedup
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

    // Step 5 - Push to processed queue
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

    // Step 6 - LLM relevance classification + signal extraction
    const llmResult = await processQueueInBatches(processedQueueKey, clientId, industry, jobId);
    await generateHighlight(clientId);

    await updateJobStage(jobId, 'llm_processing', {
      afterLlm: llmResult.relevant,
      storedFinal: llmResult.relevant,
    });

    // DONE
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