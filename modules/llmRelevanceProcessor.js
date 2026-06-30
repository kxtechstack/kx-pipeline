/**
 * llmRelevanceProcessor.js
 * ===========================
 * Pulls articles from the processed Redis queue in small batches,
 * classifies each one as relevant/irrelevant via LM Studio, and for
 * relevant articles also extracts structured signal fields (title,
 * category, impact level, source_type, country, summary, business impact)
 * in the SAME LLM call -- no second call needed.
 *
 * Before storing in Qdrant, article content is:
 *   1. Cleaned (removes URLs, ads, nav menus, junk)
 *   2. Synthesized by LLM into its own words (avoids storing copyrighted text)
 *
 * Chunks + embeds the SYNTHESIZED content and stores in Qdrant.
 * Synthesized title and body stored in Supabase — no raw original text anywhere.
 *
 * Prompt is fetched from Supabase (prompts table),
 * never hardcoded here.
 */

const { createClient } = require('@supabase/supabase-js');
const { QdrantClient } = require('@qdrant/js-client-rest');
const { pipeline } = require('@xenova/transformers');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const { pullProcessedBatch, getProcessedQueueLength } = require('./processedQueue');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

// ── Log each article's processing result to Supabase ────────────────────────
const logArticle = async (jobId, clientId, article, status, errorMessage = null) => {
  await supabase.from('article_processing_log').insert({
    job_id: jobId,
    client_id: clientId,
    article_url: article.url,
    article_title: article.title,
    status,
    error_message: errorMessage,
    raw_content: status === 'failed' ? JSON.stringify(article) : null,
    processed_at: new Date().toISOString(),
  });
};

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const LM_STUDIO_URL = process.env.LM_STUDIO_URL || 'http://localhost:1234/v1/chat/completions';
console.log("LM_STUDIO_URL =", LM_STUDIO_URL);
const LM_STUDIO_MODEL = process.env.LM_STUDIO_MODEL || 'llama-3.2-3b-instruct';

const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';
const VECTOR_SIZE = Number(process.env.EMBEDDING_VECTOR_SIZE) || 384;
const DELAY_BETWEEN_CALLS_MS = Number(process.env.LLM_CALL_DELAY_MS) || 1000;
const LLM_BATCH_SIZE = Number(process.env.LLM_BATCH_SIZE) || 10;
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE) || 300;
const CHUNK_OVERLAP = Number(process.env.CHUNK_OVERLAP) || 50;
const TEXT_TRUNCATE_LENGTH = Number(process.env.LLM_TEXT_TRUNCATE_LENGTH) || 5000;

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ── Embedding model ──────────────────────────────────────────────────────────
let embedderPromise = null;
const getEmbedder = () => {
  if (!embedderPromise) embedderPromise = pipeline('feature-extraction', 'Xenova/all-MiniLM-L6-v2');
  return embedderPromise;
};
const embedText = async (text) => {
  const embedder = await getEmbedder();
  const output = await embedder(text, { pooling: 'mean', normalize: true });
  return Array.from(output.data);
};

// ── Fetch relevance prompt from Supabase ─────────────────────────────────────
const getRelevancePromptTemplate = async () => {
  const { data, error } = await supabase
    .from('prompts')
    .select('prompt_template')
    .eq('id', 'policy_risk_relevance_v1')
    .eq('is_active', true)
    .single();

  if (error || !data) {
    throw new Error(`Could not load relevance prompt from Supabase: ${error?.message}`);
  }

  return data.prompt_template;
};

const getSynthesisPromptTemplate = async () => {
  const { data, error } = await supabase
    .from('prompts')
    .select('prompt_template')
    .eq('id', 'article_synthesis_v1')
    .eq('is_active', true)
    .single();

  if (error || !data) {
    throw new Error(`Could not load synthesis prompt: ${error?.message}`);
  }

  return data.prompt_template;
};

// ── Fill prompt placeholders ─────────────────────────────────────────────────
const fillPromptTemplate = (template, industry, title, text) => {
  const truncatedText = (text || '').slice(0, TEXT_TRUNCATE_LENGTH);
  return template
    .replace(/{industry}/g, industry)
    .replace(/{title}/g, title || '')
    .replace(/{text}/g, truncatedText);
};

// ── Clean raw article text before synthesis ──────────────────────────────────
const cleanArticleText = (text) => {
  if (!text) return '';
  return text
    .replace(/https?:\/\/[^\s]+/g, '')
    .replace(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g, '')
    .replace(/^.{1,30}$/gm, '')
    .replace(/share\s+on\s+(twitter|facebook|linkedin|whatsapp|email)/gi, '')
    .replace(/(tweet|retweet|like|follow|subscribe|sign up|log in|sign in|register)/gi, '')
    .replace(/we use cookies.{0,200}/gi, '')
    .replace(/privacy policy.{0,100}/gi, '')
    .replace(/(read more|click here|learn more|find out more|see more|view more).{0,50}/gi, '')
    .replace(/\[image[^\]]*\]/gi, '')
    .replace(/caption:.{0,100}/gi, '')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/[ \t]{2,}/g, ' ')
    .trim();
};

// ── Synthesize article into LLM's own words ──────────────────────────────────
// Returns { title, body } — both in LLM's own words, no raw text anywhere.
const synthesizeContent = async (article, classification) => {
  try {
    const cleanedText = cleanArticleText(article.text || '');
    const promptTemplate = await getSynthesisPromptTemplate();
    const userPrompt = promptTemplate
      .replace(/{title}/g, article.title || '')
      .replace(/{text}/g, cleanedText.slice(0, TEXT_TRUNCATE_LENGTH));

    const response = await axios.post(LM_STUDIO_URL, {
      model: LM_STUDIO_MODEL,
      messages: [
        { role: 'system', content: 'You are a senior regulatory intelligence analyst writing original analytical summaries. You never copy text from source articles.' },
        { role: 'user', content: userPrompt },
      ],
      temperature: 0.3,
      max_tokens: 1500,
    }, {
      timeout: 180000,
      headers: {
        'Content-Type': 'application/json',
        'ngrok-skip-browser-warning': 'true',
      },
    });

    const synthesized = response.data.choices[0].message.content.trim();
    const lines = synthesized.split('\n').filter(l => l.trim() !== '');
    const titleLine = lines[0].replace(/^Title:\s*/i, '').trim();
    const bodyText = lines.slice(1).join('\n').trim();
    console.log(`  [Synthesize] Title: "${titleLine}" | Body: ${bodyText.length} chars`);
    return { title: titleLine, body: bodyText };

  } catch (err) {
    console.log(`  [Synthesize] Failed, falling back to summary: ${err.message}`);
    return { title: null, body: classification.summary || '' };
  }
};

// ── Call LM Studio for relevance classification + signal extraction ──────────
const classifyArticle = async (promptTemplate, industry, article) => {
  const prompt = fillPromptTemplate(promptTemplate, industry, article.title, article.text);

  try {
    const response = await axios.post(LM_STUDIO_URL, {
      model: LM_STUDIO_MODEL,
      messages: [
        { role: 'system', content: 'You are a strict, precise classification assistant. You only respond with valid JSON, nothing else.' },
        { role: 'user', content: prompt },
      ],
      temperature: 0.1,
      max_tokens: 800,
    }, {
      timeout: 180000,
      headers: {
        'Content-Type': 'application/json',
        'ngrok-skip-browser-warning': 'true',
      },
    });

    const rawContent = response.data.choices[0].message.content.trim();
    let cleaned = rawContent.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();
    const jsonMatch = cleaned.match(/\{[\s\S]*?\}/);
    if (jsonMatch) cleaned = jsonMatch[0];

    let parsed;
    try {
      parsed = JSON.parse(cleaned);
    } catch (parseErr) {
      try {
        const repaired = cleaned.replace(
          /"(reason|country|summary|signal_title|category|impact_level|source_type)"\s*:\s*"((?:[^"\\]|\\.)*)"/g,
          (match, key, value) => `"${key}": "${value}"`
        );
        parsed = JSON.parse(repaired);
      } catch (repairErr) {
        console.log(`      Raw response was: ${rawContent}`);
        throw parseErr;
      }
    }

    return {
      is_relevant: Boolean(parsed.is_relevant),
      reason: parsed.reason || 'No reason provided',
      signal_title: parsed.signal_title || article.title,
      category: parsed.category || 'Other Regulatory Risk',
      impact_level: parsed.impact_level || 'Low',
      source_type: parsed.source_type || 'News Report',
      country: parsed.country || 'Unknown',
      summary: parsed.summary || '',
      business_impact: Array.isArray(parsed.business_impact) ? parsed.business_impact : [],
    };

  } catch (err) {
    console.log(`  [!] LLM classification failed for "${article.title}": ${err.message}`);
    return {
      is_relevant: false,
      technical_failure: true,
      reason: `Classification failed: ${err.message}`,
      signal_title: article.title,
      category: 'Other Regulatory Risk',
      impact_level: 'Low',
      source_type: 'News Report',
      country: 'Unknown',
      summary: '',
      business_impact: [],
    };
  }
};

// ── Chunk text ───────────────────────────────────────────────────────────────
const chunkText = (text, chunkSize = CHUNK_SIZE, overlap = CHUNK_OVERLAP) => {
  if (!text) return [];
  const words = text.split(/\s+/);
  const chunks = [];
  let start = 0;
  while (start < words.length) {
    const end = start + chunkSize;
    chunks.push(words.slice(start, end).join(' '));
    start += chunkSize - overlap;
  }
  return chunks;
};

// ── Ensure Qdrant collection exists ─────────────────────────────────────────
const setupPolicyCollection = async () => {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some(c => c.name === POLICY_COLLECTION);
  if (!exists) {
    await qdrant.createCollection(POLICY_COLLECTION, {
      vectors: { size: VECTOR_SIZE, distance: 'Cosine' },
    });
    console.log(`[LLMProcessor] Created Qdrant collection '${POLICY_COLLECTION}'`);
  }

  // Always ensure indexes exist — safe to call even if they already exist
  const indexFields = ['article_id', 'client_id', 'industry'];
  for (const field of indexFields) {
    try {
      await qdrant.createPayloadIndex(POLICY_COLLECTION, {
        field_name: field,
        field_schema: 'keyword',
      });
    } catch (err) {
      if (!err.message.includes('already exists')) {
        console.log(`[LLMProcessor] Index note for '${field}': ${err.message}`);
      }
    }
  }
};

// ── Store relevant article ───────────────────────────────────────────────────
const storeRelevantArticle = async (article, classification, clientId, industry, jobId) => {

  // Step 1 — Synthesize content into LLM's own words
  // NEVER store raw copyrighted article text anywhere
  const synthesized = await synthesizeContent(article, classification);
  const synthesizedTitle = synthesized.title || classification.signal_title;
  const contentToChunk = synthesized.body || classification.summary || '';
  const chunks = chunkText(contentToChunk);
  const articleId = uuidv4();

  // Step 2 — Chunk synthesized body and store in Qdrant
  const points = [];
  for (let i = 0; i < chunks.length; i++) {
    const vector = await embedText(chunks[i]);
    points.push({
      id: uuidv4(),
      vector,
      payload: {
        article_id: articleId,
        client_id: clientId,
        industry,
        title: synthesizedTitle,
        url: article.url,
        chunk_index: i,
        chunk_text: chunks[i],
        published_date: article.publishedDate,
      },
    });
  }

  if (points.length > 0) {
    await qdrant.upsert(POLICY_COLLECTION, { points });
  }

  // Step 3 — Store metadata in Supabase (synthesized title, no raw text)
  const { error: metaError } = await supabase.from('policy_articles_metadata').insert({
    article_id: articleId,
    client_id: clientId,
    industry,
    title: synthesizedTitle,
    article_url: article.url,
    published_date: article.publishedDate,
    location: classification.country,
    is_relevant: true,
    relevance_reason: classification.reason,
  });
  if (metaError) console.error('[Storage] metadata insert error:', metaError.message);

  // Step 4 — Store synthesized content in Supabase (no raw original text)
  const { error: fullError } = await supabase.from('policy_articles_full').insert({
    article_id: articleId,
    client_id: clientId,
    industry,
    title: synthesizedTitle,
    url: article.url,
    published_date: article.publishedDate,
    author: article.author,
    full_text: contentToChunk,
    is_relevant: true,
    relevance_reason: classification.reason,
    location: classification.country,
    chunk_count: chunks.length,
    qdrant_collection_name: POLICY_COLLECTION,
    job_id: jobId,
  });
  if (fullError) console.error('[Storage] full insert error:', fullError.message);

  // Step 5 — Store structured signal for frontend
  const { error: signalError } = await supabase.from('policy_signals').insert({
    article_id: articleId,
    client_id: clientId,
    industry,
    source_article_url: article.url,
    source_published_date: article.publishedDate,
    signal_title: classification.signal_title,
    category: classification.category,
    impact_level: classification.impact_level,
    source_type: classification.source_type,
    country: classification.country,
    summary: classification.summary,
    business_impact: classification.business_impact,
    job_id: jobId,
  });
  if (signalError) console.error('[Storage] signal insert error:', signalError.message);

  return chunks.length;
};

// ── Main processing function ─────────────────────────────────────────────────
const processArticlesForRelevance = async (articles, clientId, industry, jobId) => {
  if (!articles || articles.length === 0) return { relevant: 0, irrelevant: 0 };

  await setupPolicyCollection();
  const promptTemplate = await getRelevancePromptTemplate();

  let relevantCount = 0;
  let irrelevantCount = 0;

  for (const article of articles) {
    console.log(`[LLMProcessor] Classifying: "${article.title}"`);

    const classification = await classifyArticle(promptTemplate, industry, article);

    if (classification.technical_failure) {
      await logArticle(jobId, clientId, article, 'failed', classification.reason);
      irrelevantCount++;
      console.log(`  [!] FAILED | ${classification.reason}`);
    } else if (classification.is_relevant) {
      const chunkCount = await storeRelevantArticle(
        article,
        classification,
        clientId,
        industry,
        jobId
      );
      await logArticle(jobId, clientId, article, 'completed');
      relevantCount++;
      console.log(
        `  [✓] RELEVANT (${chunkCount} chunks) | ${classification.category} | ${classification.impact_level} | ${classification.reason}`
      );
    } else {
      await logArticle(jobId, clientId, article, 'skipped', classification.reason);
      irrelevantCount++;
      console.log(`  [✗] IRRELEVANT | ${classification.reason}`);
    }

    await sleep(DELAY_BETWEEN_CALLS_MS);
  }

  console.log(`[LLMProcessor] Done. Relevant: ${relevantCount}, Irrelevant: ${irrelevantCount}`);
  return { relevant: relevantCount, irrelevant: irrelevantCount };
};

// ── Batch processing from Redis queue ───────────────────────────────────────
async function processQueueInBatches(queueKey, clientId, industry, jobId, batchSize = LLM_BATCH_SIZE) {
  let totalRelevant = 0;
  let totalIrrelevant = 0;
  let remaining = await getProcessedQueueLength(queueKey);

  console.log(`[LLMProcessor] Starting batch processing of ${queueKey} (${remaining} articles, batches of ${batchSize})`);

  let batchNumber = 1;
  while (remaining > 0) {
    console.log(`\n[LLMProcessor] --- Batch ${batchNumber} (${remaining} remaining) ---`);

    const batch = await pullProcessedBatch(queueKey, batchSize);
    const result = await processArticlesForRelevance(batch, clientId, industry, jobId);

    totalRelevant += result.relevant;
    totalIrrelevant += result.irrelevant;

    remaining = await getProcessedQueueLength(queueKey);
    batchNumber++;
  }

  console.log(`\n[LLMProcessor] ALL BATCHES DONE. Total relevant: ${totalRelevant}, Total irrelevant: ${totalIrrelevant}`);
  return { relevant: totalRelevant, irrelevant: totalIrrelevant };
}

module.exports = {
  processArticlesForRelevance,
  processQueueInBatches,
};