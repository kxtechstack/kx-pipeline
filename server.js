require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');

const { fetchArticles } = require('./modules/fetcher');
const { sortByNewest, pushToQueue, readBatch, getQueueLength, setStatus, getStatus, acquireLock, refreshLock, releaseLock } = require('./modules/queueManager');
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
const { extractContent } = require('./modules/customSourceExtractor');
const { processCustomSource } = require('./modules/customSourceProcessor');

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

// CHANGED: New helper — looks up which module a submodule belongs to.
// Needed anywhere we only have a submoduleId (like /retry-failed) but
// need the moduleId too (e.g. to pick the right relevance prompt).
const getModuleIdForSubmodule = async (submoduleId) => {
  const { data, error } = await supabaseClient
    .schema('admin')
    .from('submodules')
    .select('module_id')
    .eq('id', submoduleId)
    .single();

  if (error || !data) {
    throw new Error(`Could not find module_id for submodule_id: ${submoduleId}`);
  }
  return data.module_id;
};
app.post('/admin/invite-user', async (req, res) => {
  const { email, clientId, firstName, lastName, designation } = req.body;

  if (!email || !clientId) {
    return res.status(400).json({ error: 'email and clientId are required' });
  }

  try {
    const { data, error } = await supabaseClient.auth.admin.inviteUserByEmail(email, {
      redirectTo: 'https://pwdcheck.sneha-9a1.workers.dev/',
      data: { client_id: clientId }
    });

    if (error) {
      console.error('[InviteUser] Supabase error:', error.message);
      return res.status(400).json({ error: error.message });
    }

    const { error: insertError } = await supabaseClient
      .schema('admin')
      .from('client_users')
      .insert({
        email: email.toLowerCase(),
        client_id: clientId,
        first_name: firstName || null,
        last_name: lastName || null,
        designation: designation || null,
        is_active: true
      });

    if (insertError) {
      console.error('[InviteUser] client_users insert error:', insertError.message);
    }

    return res.json({ message: 'Invite sent', user: data.user });
  } catch (err) {
    console.error('[InviteUser] Error:', err.message);
    return res.status(500).json({ error: err.message });
  }
});

app.post('/ask', async (req, res) => {
  try {
    const { question, clientId, industry, moduleId } = req.body; // CHANGED: added moduleId
    if (!question || !clientId || !industry || !moduleId) {
      return res.status(400).json({ error: 'question, clientId, industry, and moduleId are required' });
    }
    const result = await askQuestion(question, clientId, industry, moduleId); // CHANGED: passes moduleId
    return res.json(result);
  } catch (err) {
    console.error('[Ask] Error:', err.message);
    return res.status(500).json({ error: err.message });
  }
});

// Main pipeline trigger
// CHANGED: now requires moduleId in the request body too, not just submoduleId
app.post('/run', async (req, res) => {
  const { clientId, promptText, industry, moduleId, submoduleId, source } = req.body;

  if (!clientId || !promptText || !industry || !moduleId || !submoduleId) {
    return res.status(400).json({ error: 'clientId, promptText, industry, moduleId, and submoduleId are all required' });
  }

  const fetchSource = source || 'Exa'; // default to Exa if caller doesn't send one

  // CHANGED: acquireLock now takes (clientId, submoduleId) — matches updated queueManager.js
  const lockAcquired = await acquireLock(clientId, submoduleId);
  if (!lockAcquired) {
    return res.status(409).json({ error: 'A pipeline is already running for this client/submodule. Please wait for it to finish.' });
  }

  const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  res.json({ jobId, status: 'started' });
  runPipeline(jobId, clientId, promptText, industry, moduleId, submoduleId, fetchSource); // CHANGED: passes moduleId
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
    const { moduleId } = req.query;
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
    const filterConditions = [
      { key: 'client_id', match: { value: signal.client_id } },
      { key: 'industry', match: { value: signal.industry } },
    ];
    if (moduleId) {
      filterConditions.push({ key: 'module_id', match: { value: moduleId } });
    }

    const recommended = await qdrantClient.recommend(POLICY_COLLECTION, {
      positive: [pointId],
      limit: 20,
      with_payload: true,
      filter: {
        must: filterConditions,
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

// Latest pipeline status for a client + submodule
app.get('/client-status/:clientId', async (req, res) => {
  try {
    const { submoduleId } = req.query;
    if (!submoduleId) {
      return res.status(400).json({ error: 'submoduleId query param is required' });
    }

    const { data, error } = await supabaseClient
      .from('pipeline_job_status')
      .select('*')
      .eq('client_id', req.params.clientId)
      .eq('submodule_id', submoduleId)
      .order('started_at', { ascending: false })
      .limit(1)
      .single();

    if (error || !data) return res.json({ hasRun: false });

    const lastRun = data.completed_at || data.updated_at;
    const minutesAgo = lastRun ? Math.floor((Date.now() - new Date(lastRun)) / 60000) : null;

    return res.json({
      hasRun: true,
      jobId: data.job_id,
      status: data.status,
      currentStage: data.current_stage,
      lastRunAt: lastRun,
      minutesAgo,
      errorMessage: data.error_message || null,
      counts: {
        fetched: data.count_fetched || 0,
        afterUrlCheck: data.count_after_url_check || 0,
        afterTopicDedup: data.count_after_topic_dedup || 0,
        afterQualityFilter: data.count_after_quality_filter || 0,
        storedFinal: data.count_stored_final || 0,
      }
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// Run a single custom data source (website / pdf / file / text)
app.post('/custom-source/run/:sourceId', async (req, res) => {
  const { sourceId } = req.params;

  try {
    // Look up the source row
    const { data: source, error } = await supabaseClient
      .schema('admin')
      .from('custom_data_sources')
      .select('*')
      .eq('id', sourceId)
      .single();

    if (error || !source) {
      return res.status(404).json({ error: 'Custom data source not found' });
    }

    // Respond immediately, process in background (same pattern as /run)
    res.json({ status: 'started', sourceId });

    try {
      const extracted = await extractContent(source);
      const result = await processCustomSource(source, extracted);
      console.log(`[CustomSource] Run complete for "${source.source_name}":`, result);
    } catch (err) {
      console.error(`[CustomSource] Run failed for "${source.source_name}":`, err.message);

      await supabaseClient
        .schema('admin')
        .from('custom_data_sources')
        .update({ last_run_status: 'failed', last_run_at: new Date().toISOString() })
        .eq('id', sourceId);

      // Log this failed attempt to the run history table too (this catch
      // block covers extraction failures -- e.g. bad URL, unreachable file --
      // which happen BEFORE processCustomSource's own try/catch would log it)
      await supabaseClient.from('custom_source_run_log').insert({
        source_id: sourceId,
        client_id: source.client_id,
        source_name: source.source_name,
        status: 'failed',
        error_message: err.message,
      });
    }

  } catch (err) {
    console.error('[CustomSource] Route error:', err.message);
    return res.status(500).json({ error: err.message });
  }
});

// Retry failed articles
// CHANGED: now looks up moduleId from the submodule (via admin.submodules),
// since article_processing_log only stores submodule_id, not module_id.
app.post('/retry-failed/:clientId', async (req, res) => {
  try {
    const { clientId } = req.params;

    // Get all failed articles with retry count less than 3
    const { data: failedArticles, error } = await supabaseClient
      .from('article_processing_log')
      .select('*')
      .eq('client_id', clientId)
      .eq('status', 'failed')
      .lt('retry_count', 3);

    if (error) return res.status(500).json({ error: error.message });
    if (!failedArticles || failedArticles.length === 0) {
      return res.json({ message: 'No failed articles to retry', count: 0 });
    }

    res.json({ message: `Retrying ${failedArticles.length} articles`, count: failedArticles.length });

    // Process each failed article
    for (const record of failedArticles) {
      try {
        if (!record.raw_content) {
          await supabaseClient
            .from('article_processing_log')
            .update({ status: 'permanently_failed', error_message: 'No raw content saved for retry' })
            .eq('id', record.id);
          continue;
        }

        const article = JSON.parse(record.raw_content);

        // Get industry from pipeline_job_status
        const { data: job } = await supabaseClient
          .from('pipeline_job_status')
          .select('*')
          .eq('job_id', record.job_id)
          .single();

        const industry = job?.industry || 'General';

        // CHANGED: derive moduleId from the submodule since we only have submodule_id here
        const moduleId = await getModuleIdForSubmodule(record.submodule_id);

        // Update retry count
        await supabaseClient
          .from('article_processing_log')
          .update({ retry_count: record.retry_count + 1 })
          .eq('id', record.id);

        // Re-run LLM
        const { processArticlesForRelevance } = require('./modules/llmRelevanceProcessor');
        const result = await processArticlesForRelevance(
          [article], clientId, industry, record.job_id, moduleId, record.submodule_id // CHANGED: added moduleId
        );

        // Update status based on result
        if (result.relevant > 0) {
          await supabaseClient
            .from('article_processing_log')
            .update({ status: 'completed', error_message: null })
            .eq('id', record.id);
        } else if (record.retry_count + 1 >= 3) {
          await supabaseClient
            .from('article_processing_log')
            .update({ status: 'permanently_failed' })
            .eq('id', record.id);
        }

      } catch (err) {
        console.error(`[Retry] Failed for ${record.article_url}:`, err.message);
      }
    }

  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// CHANGED: runPipeline now takes moduleId, threads it through dedup calls
// and processQueueInBatches. Also tracks currentStage so a crash logs the
// REAL stage it failed at, instead of the hardcoded 'unknown' from before.
const runPipeline = async (jobId, clientId, promptText, industry, moduleId, submoduleId, source) => {

  let currentStage = 'starting'; // CHANGED: new — tracks real stage for failJobTracking

  try {

    await startJobTracking(jobId, clientId, promptText, submoduleId);
    await setStatus(jobId, { status: 'fetching', message: `Calling ${source} API...` });
    currentStage = 'fetching'; // CHANGED

    // Step 1 - Fetch from selected source
    const articles = await fetchArticles(source, promptText);
    console.log(`\n========== PROMPT SENT TO ${source.toUpperCase()} ==========\n`);
    console.log(promptText);
    console.log("Industry:", industry);

    await updateJobStage(jobId, 'fetching', { fetched: articles.length });
    await setStatus(jobId, { status: 'sorting', total: articles.length, message: `Fetched ${articles.length} articles` });

    // Step 2 - Sort newest first
    const sorted = sortByNewest(articles);
    console.log("\n========== FIRST 90 SORTED ARTICLES ==========\n");

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
    currentStage = 'queued'; // CHANGED

    // Step 4 - URL dedup in batches of 10
    let allCleanArticles = [];
    let processedCount = 0;
    const totalRawCount = await getQueueLength(queueKey);
    let startIndex = 0;

    currentStage = 'url_dedup'; // CHANGED
    while (startIndex < totalRawCount) {
      const batch = await readBatch(queueKey, startIndex, 10);
      // CHANGED: removeUrlDuplicates now scoped by moduleId, not just clientId
      const afterUrlCheck = await removeUrlDuplicates(batch, clientId, moduleId);
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
    currentStage = 'topic_dedup'; // CHANGED

    // CHANGED: removeSameTopicArticles now scoped by moduleId, not just clientId
    const finalArticles = await removeSameTopicArticles(allCleanArticles, clientId, moduleId);

    // Level 3 - Quality filter
    await updateJobStage(jobId, 'topic_dedup', { afterTopicDedup: finalArticles.length });
    await setStatus(jobId, {
      status: 'quality_filter',
      total: sorted.length,
      message: 'Running quality filter (length, language, freshness)...'
    });
    currentStage = 'quality_filter'; // CHANGED

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
    currentStage = 'llm_processing'; // CHANGED

    // Step 6 - LLM relevance classification + signal extraction
    // CHANGED: processQueueInBatches now takes moduleId before submoduleId
    const llmResult = await processQueueInBatches(processedQueueKey, clientId, industry, jobId, moduleId, submoduleId);
    await generateHighlight(clientId, moduleId); // CHANGED: now passes moduleId

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
    await failJobTracking(jobId, currentStage, error.message); // CHANGED: was 'unknown', now the real stage
  } finally {
    // CHANGED: releaseLock now takes (clientId, submoduleId) — matches updated queueManager.js
    await releaseLock(clientId, submoduleId);
    console.log(`Lock released for client: ${clientId}, submodule: ${submoduleId}`);
  }
};


const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`KX Pipeline server running on port ${PORT}`);
});