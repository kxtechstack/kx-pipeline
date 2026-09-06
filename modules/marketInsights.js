const { createClient } = require('@supabase/supabase-js');
const { callLLM } = require('./llmClient');

const { pipeline } = require('@xenova/transformers');


const { QdrantClient } = require('@qdrant/js-client-rest');

const qdrantClient = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const INSIGHT_CENTROID_COLLECTION = process.env.INSIGHT_CENTROID_QDRANT_COLLECTION || 'market_insights_centroids';

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

const cosineSimilarity = (a, b) => {
  let dot = 0, normA = 0, normB = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
};

const CARD_SIMILARITY_THRESHOLD = 0.68;
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const setupInsightCentroidCollection = async () => {
  const collections = await qdrantClient.getCollections();
  const exists = collections.collections.some(c => c.name === INSIGHT_CENTROID_COLLECTION);

  if (!exists) {
    await qdrantClient.createCollection(INSIGHT_CENTROID_COLLECTION, {
      vectors: { size: 384, distance: 'Cosine' },
    });
    console.log(`[MarketInsights] Created Qdrant collection '${INSIGHT_CENTROID_COLLECTION}'`);
  }

  const indexFields = ['insight_id', 'module_id', 'client_id', 'industry', 'submodule_id'];
  for (const field of indexFields) {
    try {
      await qdrantClient.createPayloadIndex(INSIGHT_CENTROID_COLLECTION, {
        field_name: field,
        field_schema: 'keyword',
      });
    } catch (err) {
      if (!err.message.includes('already exists')) {
        console.log(`[MarketInsights] Index note for '${field}': ${err.message}`);
      }
    }
  }
};

// ── Compute and store a card's centroid ──────────────────────────────────────
// Mirrors updateTrendCentroid() in trendClustering.js. Called whenever a
// card is created or enriched (a new article joins it). Averages the
// embeddings of every article currently linked to this card via
// market_insight_members, and upserts that average vector as the card's
// centroid in market_insights_centroids.
const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';

const updateInsightCentroid = async (insightId) => {
  // Step 1 — get the card's own metadata (client_id, module_id, industry,
  // existing centroid_point_id if any)
  const { data: insight, error } = await supabase
    .from('market_insights')
    .select('client_id, module_id, submodule_id, centroid_point_id')
    .eq('id', insightId)
    .single();

  if (error || !insight) {
    console.log(`  [Centroid] Could not load insight ${insightId}: ${error?.message}`);
    return;
  }

  // market_insights has no industry column — look it up from the client record
  const { data: clientRow, error: clientError } = await supabase
    .schema('admin')
    .from('clients')
    .select('industry')
    .eq('id', insight.client_id)
    .single();

  if (clientError || !clientRow) {
    console.log(`  [Centroid] Could not resolve industry for client ${insight.client_id}: ${clientError?.message}`);
    return;
  }

  const industry = clientRow.industry;

  // Step 2 — get every article_id linked to this card
  const { data: members, error: membersError } = await supabase
    .from('market_insight_members')
    .select('article_id')
    .eq('insight_id', insightId);

  if (membersError || !members || members.length === 0) {
    console.log(`  [Centroid] No members found for insight ${insightId}`);
    return;
  }

  const articleIds = members.map(m => m.article_id);

  // Step 3 — pull those articles' chunk vectors from the main collection
  const points = await qdrantClient.scroll(POLICY_COLLECTION, {
    filter: {
      must: [{ key: 'article_id', match: { any: articleIds } }],
    },
    with_vector: true,
    with_payload: false,
    limit: 500,
  });

  if (!points.points || points.points.length === 0) {
    console.log(`  [Centroid] No vectors found in Qdrant for insight ${insightId}`);
    return;
  }

  // Step 4 — average all chunk vectors into one centroid
  const vectors = points.points.map(p => p.vector);
  const dim = vectors[0].length;
  const centroid = new Array(dim).fill(0);

  for (const vec of vectors) {
    for (let i = 0; i < dim; i++) centroid[i] += vec[i];
  }
  for (let i = 0; i < dim; i++) centroid[i] /= vectors.length;

  // Step 5 — reuse the existing centroid point ID if this card already has
  // one, otherwise generate a new one and save it back onto the card row
  const { v4: uuidv4 } = require('uuid');
  const centroidPointId = insight.centroid_point_id || uuidv4();

  await qdrantClient.upsert(INSIGHT_CENTROID_COLLECTION, {
    points: [{
      id: centroidPointId,
      vector: centroid,
      payload: {
        insight_id: insightId,
        module_id: insight.module_id,
        submodule_id: insight.submodule_id,
        client_id: insight.client_id,
        industry,
      },
    }],
  });

  if (!insight.centroid_point_id) {
    await supabase
      .from('market_insights')
      .update({ centroid_point_id: centroidPointId })
      .eq('id', insightId);
  }

  console.log(`  [Centroid] Updated centroid for insight ${insightId}`);
};

// ── Delete a card's centroid ──────────────────────────────────────────────
// Call this any time a market_insights row is deleted (test cleanup, admin
// action, future dedup fixes, etc.) so Qdrant never drifts out of sync with
// Supabase again. Mirrors updateInsightCentroid's lookup pattern, but for
// removal instead of upsert. IMPORTANT: call this BEFORE deleting the
// market_insights row, since it needs to read centroid_point_id from it.
const deleteInsightCentroid = async (insightId) => {
  const { data: insight, error } = await supabase
    .from('market_insights')
    .select('centroid_point_id')
    .eq('id', insightId)
    .single();

  if (error || !insight || !insight.centroid_point_id) {
    console.log(`  [Centroid] No centroid to delete for insight ${insightId}`);
    return;
  }

  await qdrantClient.delete(INSIGHT_CENTROID_COLLECTION, {
    points: [insight.centroid_point_id],
  });

  console.log(`  [Centroid] Deleted centroid for insight ${insightId}`);
};

// ── Find similar market insight cards ────────────────────────────────────────
// Mirrors findSimilarTrends() in trendClustering.js. Given one card's ID,
// finds its centroid and searches for other cards whose centroids are
// semantically close — used for the "Similar Market Movements" section
// on a card's detail view in the frontend.
const SIMILAR_INSIGHTS_LIMIT = 3; // how many related cards to surface per card

const findSimilarInsights = async (insightId) => {
  // Step 1 — get this card's own metadata + centroid point ID
  const { data: insight, error } = await supabase
    .from('market_insights')
    .select('client_id, module_id, submodule_id, centroid_point_id')
    .eq('id', insightId)
    .single();

  if (error || !insight || !insight.centroid_point_id) {
    console.log(`  [SimilarInsights] No centroid found for insight ${insightId}, skipping`);
    return [];
  }

  // industry isn't stored on market_insights — same lookup as updateInsightCentroid
  const { data: clientRow, error: clientError } = await supabase
    .schema('admin')
    .from('clients')
    .select('industry')
    .eq('id', insight.client_id)
    .single();

  if (clientError || !clientRow) {
    console.log(`  [SimilarInsights] Could not resolve industry for client ${insight.client_id}`);
    return [];
  }
  const industry = clientRow.industry;

  // Step 2 — fetch that centroid's actual vector from Qdrant
  const points = await qdrantClient.retrieve(INSIGHT_CENTROID_COLLECTION, {
    ids: [insight.centroid_point_id],
    with_vector: true,
  });

  if (!points || points.length === 0) {
    console.log(`  [SimilarInsights] Centroid vector not found in Qdrant for insight ${insightId}`);
    return [];
  }

  const ownVector = points[0].vector;

  // Step 3 — search for other centroids close to this one, excluding itself
  const searchResult = await qdrantClient.search(INSIGHT_CENTROID_COLLECTION, {
    vector: ownVector,
    filter: {
      must: [
        { key: 'module_id', match: { value: insight.module_id } },
        { key: 'client_id', match: { value: insight.client_id } },
        { key: 'industry', match: { value: industry } },
      ],
    },
    limit: SIMILAR_INSIGHTS_LIMIT + 1, // +1 since it'll match itself first
    with_payload: true,
  });

  const otherInsightIds = searchResult
    .filter(r => r.payload.insight_id !== insightId)
    .slice(0, SIMILAR_INSIGHTS_LIMIT)
    .map(r => ({ insight_id: r.payload.insight_id, score: r.score }));

  if (otherInsightIds.length === 0) {
    console.log(`  [SimilarInsights] No similar insights found for ${insightId}`);
    return [];
  }

  // Step 4 — get the actual titles for those card IDs
  const { data: relatedInsights } = await supabase
    .from('market_insights')
    .select('id, title')
    .in('id', otherInsightIds.map(t => t.insight_id));

  const titleById = {};
  (relatedInsights || []).forEach(t => { titleById[t.id] = t.title; });

  const results = otherInsightIds.map(t => ({
    insight_id: t.insight_id,
    title: titleById[t.insight_id] || 'Untitled Insight',
    score: t.score,
  }));

  console.log(`  [SimilarInsights] Insight ${insightId} — found ${results.length} similar insight(s)`);
  return results;
};

const getDimensionName = async (submoduleId) => {
  const { data } = await supabase
    .schema('admin')
    .from('submodules')
    .select('submodule_name')
    .eq('id', submoduleId)
    .single();
  return data?.submodule_name || null;
};

// Repairs common LLM JSON formatting issues before parsing (small models
// occasionally return slightly malformed JSON — stray commas, unescaped
// quotes, truncated output). Local copy, not shared with trendClustering.js.
const repairAndParseJson = (rawContent) => {
  let cleaned = rawContent.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();

  const firstBrace = cleaned.indexOf('{');
  if (firstBrace !== -1) {
    let depth = 0;
    let endIndex = -1;
    for (let i = firstBrace; i < cleaned.length; i++) {
      if (cleaned[i] === '{') depth++;
      else if (cleaned[i] === '}') {
        depth--;
        if (depth === 0) { endIndex = i; break; }
      }
    }
    if (endIndex !== -1) cleaned = cleaned.slice(firstBrace, endIndex + 1);
  }

  cleaned = cleaned
    .replace(/\\u(?![0-9a-fA-F]{4})/g, '')
    .replace(/[\u0000-\u001F]+/g, ' ')
    .replace(/\]"\s*\}/g, ']}')
    .replace(/"\s*\}\s*\}/g, '"}');

  try {
    return JSON.parse(cleaned);
  } catch (parseErr) {
    try {
      return JSON.parse(cleaned.replace(/\\"/g, '"'));
    } catch (unescapeErr) {
      try {
        const repaired = cleaned.replace(
          /"(title|summary|country)"\s*:\s*"((?:[^"\\]|\\.)*)"/g,
          (match, key, value) => `"${key}": "${value}"`
        );
        return JSON.parse(repaired);
      } catch (repairErr) {
        let repaired2 = cleaned;
        const quoteCount = (repaired2.match(/(?<!\\)"/g) || []).length;
        if (quoteCount % 2 !== 0) repaired2 += '"';
        const openBrackets = (repaired2.match(/\[/g) || []).length;
        const closeBrackets = (repaired2.match(/\]/g) || []).length;
        for (let i = 0; i < openBrackets - closeBrackets; i++) repaired2 += ']';
        const openBraces = (repaired2.match(/\{/g) || []).length;
        const closeBraces = (repaired2.match(/\}/g) || []).length;
        for (let i = 0; i < openBraces - closeBraces; i++) repaired2 += '}';
        return JSON.parse(repaired2);
      }
    }
  }
};

// Deterministic — never asked from the LLM. Strategic Relevance is
// confidence based on how many corroborating signals back this card.
const calculateRelevanceLevel = (signalCount) => {
  if (signalCount >= 7) return 'Critical';
  if (signalCount >= 4) return 'High';
  if (signalCount >= 2) return 'Medium';
  return 'Low';
};

// Step 1 — find the most similar EXISTING card, if any crosses the
// similarity threshold. CHANGED: now compares against each card's
// STABLE stored centroid in market_insights_centroids via one Qdrant
// search, instead of re-embedding title+summary live for every
// candidate every time (title/summary text shifts on every rewrite,
// which made scores unstable). Also no longer hard-filtered to the
// same signal_id — searches by submodule instead, so two articles
// about the same real event classified under slightly different
// signals can still find each other.
const findExistingInsight = async (clientId, moduleId, submoduleId, articleEmbedding, organization) => {
  // Priority 1 — if this organization already has a card in scope, always
  // use it. Checked BEFORE embedding search so a same-company signal can
  // never get hijacked by an unrelated card that happens to score higher
  // on shared boilerplate wording.
  if (organization && organization !== 'Unknown') {
    const { data: orgSignal } = await supabase
      .from('market_dynamics_signals')
      .select('insight_id')
      .eq('client_id', clientId)
      .eq('module_id', moduleId)
      .eq('submodule_id', submoduleId)
      .eq('organization', organization)
      .not('insight_id', 'is', null)
      .order('published_date', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (orgSignal && orgSignal.insight_id) {
      const { data: card } = await supabase
        .from('market_insights')
        .select('*')
        .eq('id', orgSignal.insight_id)
        .single();
      if (card) {
        console.log(`  [CardMatch] Org match for "${organization}" -> card ${card.id}`);
        return card;
      }
    }
  }

  // Priority 2 — no card for this organization yet: fall back to
  // embedding similarity, which is what lets different companies with a
  // shared theme still land on one card together.
  const searchResult = await qdrantClient.search(INSIGHT_CENTROID_COLLECTION, {
    vector: articleEmbedding,
    filter: {
      must: [
        { key: 'client_id', match: { value: clientId } },
        { key: 'module_id', match: { value: moduleId } },
        { key: 'submodule_id', match: { value: submoduleId } },
      ],
    },
    limit: 1,
    with_payload: true,
  });

  const bestMatch = searchResult[0];

  if (!bestMatch) {
    console.log(`  [CardMatch] No existing cards to compare against yet`);
    return null;
  }

  const { count: memberCount } = await supabase
    .from('market_insight_members')
    .select('*', { count: 'exact', head: true })
    .eq('insight_id', bestMatch.payload.insight_id);

  console.log(`  [CardMatch] score=${bestMatch.score.toFixed(3)} threshold=${CARD_SIMILARITY_THRESHOLD} card=${bestMatch.payload.insight_id} existing_members=${memberCount}`);

  if (bestMatch.score < CARD_SIMILARITY_THRESHOLD) return null;

  const { data: card } = await supabase
    .from('market_insights')
    .select('*')
    .eq('id', bestMatch.payload.insight_id)
    .single();

  return card || null;
};

// Step 2 — ask the LLM to write (or rewrite) the card.
// existingCard is null on first creation.
// Strips markdown bold and a leaked "Title:" prefix the model sometimes
// includes inside the JSON value itself (seen with gpt-oss-20b).
const cleanText = (text) => {
  if (!text) return text;
  return text
    .replace(/^\*+/, '').replace(/\*+$/, '')  // strip leading/trailing *'s
    .replace(/^title:\s*/i, '')                // strip leaked "Title:" prefix
    .trim();
};
const generateInsightWriteup = async (existingCard, newArticleText, industry) => {
  const { data: promptRow, error } = await supabase
    .from('prompts')
    .select('prompt_template')
    .eq('id', 'market_dynamics_writeup_v1')
    .eq('is_active', true)
    .single();

  if (error || !promptRow) {
    throw new Error(`Could not load market_dynamics_writeup_v1 prompt: ${error?.message}`);
  }

  let companiesCovered = [];
  if (existingCard) {
    const { data: memberSignals } = await supabase
      .from('market_dynamics_signals')
      .select('organization')
      .eq('insight_id', existingCard.id);
    companiesCovered = [...new Set((memberSignals || []).map(s => s.organization).filter(Boolean))];
  }

  const existingText = existingCard
    ? `EXISTING CARD:\nTitle: ${existingCard.title}\nSummary: ${existingCard.summary}\nCompanies already covered by this card so far: ${companiesCovered.join(', ') || 'unknown'}`
    : 'EXISTING CARD: none — this is the first signal for this topic.';

  const finalPrompt = promptRow.prompt_template
  .replace(/{industry}/g, industry)
  .replace(/{existing_card}/g, existingText)
  .replace(/{new_article}/g, newArticleText);

  const raw = await callLLM([
    { role: 'system', content: 'You only respond with valid JSON, nothing else.' },
    { role: 'user', content: finalPrompt },
  ], { temperature: 0.4, max_tokens: 600, timeout: 90000 });

  const cleaned = raw.replace(/^```json\s*/i, '').replace(/```\s*$/i, '').trim();
  const parsed = repairAndParseJson(raw);

  return {
    title: cleanText(parsed.title),
    summary: cleanText(parsed.summary),
    short_summary: cleanText(parsed.short_summary),
    business_impact: Array.isArray(parsed.business_impact) ? parsed.business_impact.map(cleanText) : [],
    country: parsed.country || existingCard?.country || 'Global',
  };
};

// Step 3 — the main entry point. Called once per relevant Market Dynamics article.
const enrichOrCreateInsight = async (clientId, moduleId, submoduleId, signalId, articleId, articleText, industry, organization) => {
  await setupInsightCentroidCollection(); // NEW — ensures submodule_id index exists before we search on it
  const articleEmbedding = await embedText((articleText || '').slice(0, 4000));
  const existing = await findExistingInsight(clientId, moduleId, submoduleId, articleEmbedding, organization);

  const writeup = await generateInsightWriteup(existing, articleText, industry);
  const category = await getDimensionName(submoduleId);

  if (existing) {
    await supabase
      .from('market_insights')
      .update({
        title: writeup.title,
        summary: writeup.summary,
        short_summary: writeup.short_summary,
        business_impact: writeup.business_impact,
        country: writeup.country,
        category,
        signal_count: existing.signal_count + 1,
        relevance_level: calculateRelevanceLevel(existing.signal_count + 1),
        last_enriched_at: new Date().toISOString(),
      })
      .eq('id', existing.id);

    await supabase.from('market_insight_members').insert({ insight_id: existing.id, article_id: articleId });
    await updateInsightCentroid(existing.id);
    console.log(`  [MarketDynamics] Enriched existing card "${writeup.title}" (now ${existing.signal_count + 1} signals)`);
    return { status: 'enriched', insightId: existing.id };
  }

  const { data: newInsight, error } = await supabase
    .from('market_insights')
    .insert({
      client_id: clientId,
      module_id: moduleId,
      submodule_id: submoduleId,
      signal_id: signalId,
      title: writeup.title,
      summary: writeup.summary,
      short_summary: writeup.short_summary,
      business_impact: writeup.business_impact,
      country: writeup.country,
      category,
      signal_count: 1,
      relevance_level: calculateRelevanceLevel(1),
      last_enriched_at: new Date().toISOString(),
    })
    .select()
    .single();

  if (error) {
    console.error('  [MarketDynamics] Failed to create insight:', error.message);
    return { status: 'error', error: error.message };
  }

  await supabase.from('market_insight_members').insert({ insight_id: newInsight.id, article_id: articleId });
  await updateInsightCentroid(newInsight.id);
  console.log(`  [MarketDynamics] Created new card "${writeup.title}"`);
  return { status: 'created', insightId: newInsight.id };
};

module.exports = { findExistingInsight, generateInsightWriteup, enrichOrCreateInsight, setupInsightCentroidCollection, updateInsightCentroid, findSimilarInsights, deleteInsightCentroid };