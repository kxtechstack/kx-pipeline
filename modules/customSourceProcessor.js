/**
 * customSourceProcessor.js
 * ==========================
 * Takes raw extracted text (from customSourceExtractor.js) and:
 *   1. Synthesizes it into original wording via LLM (copyright safety --
 *      same pattern as llmRelevanceProcessor.js's synthesizeContent)
 *   2. Chunks the synthesized text
 *   3. Embeds each chunk
 *   4. Stores chunks in a DEDICATED Qdrant collection (separate from
 *      policy_articles -- this is general knowledge, not a policy signal)
 *   5. Stores the synthesized full text in Postgres (custom_source_content)
 *   6. Updates custom_data_sources.last_run_status / last_run_at
 *
 * This is intentionally simpler than llmRelevanceProcessor.js --
 * no relevance classification, no module/industry tagging, no
 * impact_level. Just "make this content searchable."
 */

const { createClient } = require('@supabase/supabase-js');
const { QdrantClient } = require('@qdrant/js-client-rest');
const { pipeline } = require('@xenova/transformers');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const LM_STUDIO_URL = process.env.LM_STUDIO_URL || 'http://localhost:1234/v1/chat/completions';
const LM_STUDIO_MODEL = process.env.LM_STUDIO_MODEL || 'llama-3.2-3b-instruct';

const CUSTOM_COLLECTION = process.env.CUSTOM_SOURCE_QDRANT_COLLECTION || 'custom_source_content';
const VECTOR_SIZE = Number(process.env.EMBEDDING_VECTOR_SIZE) || 384;
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE) || 300;
const CHUNK_OVERLAP = Number(process.env.CHUNK_OVERLAP) || 50;
const TEXT_TRUNCATE_LENGTH = Number(process.env.LLM_TEXT_TRUNCATE_LENGTH) || 5000;

// ── Embedding model (shared pattern with llmRelevanceProcessor.js) ──────────
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

// ── Chunk text ────────────────────────────────────────────────────────────
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

// ── Fetch synthesis prompt from Supabase ─────────────────────────────────
const getSynthesisPromptTemplate = async () => {
  const { data, error } = await supabase
    .from('prompts')
    .select('prompt_template')
    .eq('id', 'custom_source_synthesis_v1')
    .eq('is_active', true)
    .single();

  if (error || !data) {
    throw new Error(`Could not load custom source synthesis prompt from Supabase: ${error?.message}`);
  }

  return data.prompt_template;
};

// ── Synthesize raw text into original wording ────────────────────────────
const synthesizeContent = async (title, rawText) => {
  const truncated = (rawText || '').slice(0, TEXT_TRUNCATE_LENGTH);

  const systemPrompt = 'You are a meticulous analyst who rewrites source documents into clear, original summaries. You NEVER copy sentences verbatim -- you always express content in your own words. Above all, you NEVER change, merge, or misattribute any number, statistic, date, or figure. Every number in your summary must stay attached to the exact same label/metric it had in the source. If you are not certain which label a number belongs to, omit that number rather than guessing.';

  const promptTemplate = await getSynthesisPromptTemplate();
  const userPrompt = promptTemplate
    .replace(/{title}/g, title || 'Untitled')
    .replace(/{text}/g, truncated);

  try {
    const response = await axios.post(LM_STUDIO_URL, {
      model: LM_STUDIO_MODEL,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ],
      temperature: 0.1,
      max_tokens: 1800,
    }, {
      timeout: 180000,
      headers: {
        'Content-Type': 'application/json',
        'ngrok-skip-browser-warning': 'true',
      },
    });

    let synthesized = response.data.choices[0].message.content.trim();

    // Strip common preamble/closing patterns the model sometimes adds
    // despite instructions, as a safety net.
    synthesized = synthesized
      .replace(/^(here'?s|below is|the following is)[^\n]*\n+/i, '')
      .replace(/\n+note:.*$/is, '')
      .trim();

    return synthesized;
  } catch (err) {
    console.log(`[CustomSourceProcessor] Synthesis failed, falling back to truncated raw text: ${err.message}`);
    // Fallback: still don't store the full raw text, just a short truncated slice
    return truncated.slice(0, 2000);
  }
};

// ── Ensure the custom source Qdrant collection exists ────────────────────
const setupCustomCollection = async () => {
  const collections = await qdrant.getCollections();
  const exists = collections.collections.some(c => c.name === CUSTOM_COLLECTION);
  if (!exists) {
    await qdrant.createCollection(CUSTOM_COLLECTION, {
      vectors: { size: VECTOR_SIZE, distance: 'Cosine' },
    });
    console.log(`[CustomSourceProcessor] Created Qdrant collection '${CUSTOM_COLLECTION}'`);
  }

  const indexFields = ['client_id', 'source_id'];
  for (const field of indexFields) {
    try {
      await qdrant.createPayloadIndex(CUSTOM_COLLECTION, {
        field_name: field,
        field_schema: 'keyword',
      });
    } catch (err) {
      if (!err.message.includes('already exists')) {
        console.log(`[CustomSourceProcessor] Index note for '${field}': ${err.message}`);
      }
    }
  }
};

// ── Mark the source row's run status ─────────────────────────────────────
const markRunStatus = async (sourceId, status, articleId = null) => {
  const { error } = await supabase
    .schema('admin')
    .from('custom_data_sources')
    .update({
      last_run_status: status,
      last_run_at: new Date().toISOString(),
      last_article_id: articleId,
    })
    .eq('id', sourceId);

  if (error) {
    console.error(`[CustomSourceProcessor] Failed to update run status for source ${sourceId}:`, error.message);
  }
};

// ── Main entry point ──────────────────────────────────────────────────────
/**
 * @param {object} source - the full row from admin.custom_data_sources
 * @param {object} extracted - { title, text } from customSourceExtractor.js
 */
const processCustomSource = async (source, extracted) => {
  try {
    await setupCustomCollection();

    // Step 1 -- synthesize into original words
    const synthesized = await synthesizeContent(extracted.title, extracted.text);

    // Step 2 -- chunk
    const chunks = chunkText(synthesized);
    if (chunks.length === 0) {
      throw new Error('No content left to store after synthesis/chunking');
    }

    const contentId = uuidv4();

    // Step 3 -- embed + store each chunk in Qdrant
    const points = [];
    for (let i = 0; i < chunks.length; i++) {
      const vector = await embedText(chunks[i]);
      points.push({
        id: uuidv4(),
        vector,
        payload: {
          content_id: contentId,
          client_id: source.client_id,
          source_id: source.id,
          source_name: source.source_name,
          source_type: source.source_type,
          title: extracted.title,
          chunk_index: i,
          chunk_text: chunks[i],
        },
      });
    }
    await qdrant.upsert(CUSTOM_COLLECTION, { points });

    // Step 4 -- store full synthesized text in Postgres
    const { error: insertError } = await supabase.from('custom_source_content').insert({
      id: contentId,
      source_id: source.id,
      client_id: source.client_id,
      title: extracted.title,
      synthesized_content: synthesized,
      chunk_count: chunks.length,
      qdrant_collection_name: CUSTOM_COLLECTION,
    });
    if (insertError) throw new Error(`Postgres insert failed: ${insertError.message}`);

    // Step 5 -- mark success
    await markRunStatus(source.id, 'success', contentId);

    console.log(`[CustomSourceProcessor] Done. Source "${source.source_name}" -> ${chunks.length} chunks stored.`);
    return { success: true, contentId, chunkCount: chunks.length };

  } catch (err) {
    console.error(`[CustomSourceProcessor] Failed for source "${source.source_name}":`, err.message);
    await markRunStatus(source.id, 'failed', null);
    return { success: false, error: err.message };
  }
};

module.exports = { processCustomSource };