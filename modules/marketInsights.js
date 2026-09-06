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

// Used ONLY for cross-company topic matching (different org, same signal).
// Deliberately strict -- this is what stops "same sentence template,
// different company" false merges. Same-company matching below never uses
// this at all, so it can never block a same-company merge.
const CARD_SIMILARITY_THRESHOLD = 0.90;

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
// FROZEN: only the first 3 founding members (by join order) ever contribute
// to the centroid. This stops the centroid drifting toward a generic
// "topic average" as a card grows, which was making blobs easier to join
// the more members they already had.
const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';

const updateInsightCentroid = async (insightId) => {
  const { data: insight, error } = await supabase
    .from('market_insights')
    .select('client_id, module_id, submodule_id, centroid_point_id')
    .eq('id', insightId)
    .single();

  if (error || !insight) {
    console.log(`  [Centroid] Could not load insight ${insightId}: ${error?.message}`);
    return;
  }

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

  const { data: members, error: membersError } = await supabase
    .from('market_insight_members')
    .select('article_id, joined_at')
    .eq('insight_id', insightId)
    .order('joined_at', { ascending: true })
    .limit(3);

  if (membersError || !members || members.length === 0) {
    console.log(`  [Centroid] No members found for insight ${insightId}`);
    return;
  }

  const articleIds = members.map(m => m.article_id);

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

  const vectors = points.points.map(p => p.vector);
  const dim = vectors[0].length;
  const centroid = new Array(dim).fill(0);

  for (const vec of vectors) {
    for (let i = 0; i < dim; i++) centroid[i] += vec[i];
  }
  for (let i = 0; i < dim; i++) centroid[i] /= vectors.length;

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

const SIMILAR_INSIGHTS_LIMIT = 3;

const findSimilarInsights = async (insightId) => {
  const { data: insight, error } = await supabase
    .from('market_insights')
    .select('client_id, module_id, submodule_id, centroid_point_id')
    .eq('id', insightId)
    .single();

  if (error || !insight || !insight.centroid_point_id) {
    console.log(`  [SimilarInsights] No centroid found for insight ${insightId}, skipping`);
    return [];
  }

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

  const points = await qdrantClient.retrieve(INSIGHT_CENTROID_COLLECTION, {
    ids: [insight.centroid_point_id],
    with_vector: true,
  });

  if (!points || points.length === 0) {
    console.log(`  [SimilarInsights] Centroid vector not found in Qdrant for insight ${insightId}`);
    return [];
  }

  const ownVector = points[0].vector;

  const searchResult = await qdrantClient.search(INSIGHT_CENTROID_COLLECTION, {
    vector: ownVector,
    filter: {
      must: [
        { key: 'module_id', match: { value: insight.module_id } },
        { key: 'client_id', match: { value: insight.client_id } },
        { key: 'industry', match: { value: industry } },
      ],
    },
    limit: SIMILAR_INSIGHTS_LIMIT + 1,
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

const calculateRelevanceLevel = (signalCount) => {
  if (signalCount >= 7) return 'Critical';
  if (signalCount >= 4) return 'High';
  if (signalCount >= 2) return 'Medium';
  return 'Low';
};

// ── Find the most similar EXISTING card ──────────────────────────────────
// TWO-TIER matching:
//
// TIER 1 — SAME COMPANY (deterministic, no embedding, always correct):
// If this article's organization already has an active card in this exact
// client/module/submodule scope, always reuse that card. A company's own
// signals should never split apart just because the LLM phrased two
// articles differently -- this guarantees they never do.
//
// TIER 2 — SAME SIGNAL, different company (topic clustering):
// Only runs if Tier 1 found nothing. Restricts candidates to cards that
// share the SAME signal_id (not just the same submodule -- submodule is
// too broad and was letting unrelated signal types collide). Requires a
// STRICT similarity score (see CARD_SIMILARITY_THRESHOLD) since this is
// where "same sentence template, different company" false merges happen.
const findExistingInsight = async (clientId, moduleId, submoduleId, signalId, articleEmbedding, organization) => {
  // TIER 1 — exact organization match
  if (organization && organization !== 'Unknown') {
    const { data: orgSignal } = await supabase
      .from('market_dynamics_signals')
      .select('insight_id')
      .eq('client_id', clientId)
      .eq('module_id', moduleId)
      .eq('submodule_id', submoduleId)
      .eq('organization', organization)
      .eq('signal_id', signalId)
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
        console.log(`  [CardMatch] TIER1 org match for "${organization}" -> card ${card.id}`);
        return card;
      }
    }
  }

  // TIER 2 — same signal, embedding similarity, strict threshold
  const searchResult = await qdrantClient.search(INSIGHT_CENTROID_COLLECTION, {
    vector: articleEmbedding,
    filter: {
      must: [
        { key: 'client_id', match: { value: clientId } },
        { key: 'module_id', match: { value: moduleId } },
        { key: 'submodule_id', match: { value: submoduleId } },
      ],
    },
    limit: 5,
    with_payload: true,
  });

  if (searchResult.length === 0) {
    console.log(`  [CardMatch] TIER2 no existing cards to compare against yet`);
    return null;
  }

  for (const candidate of searchResult) {
    if (candidate.score < CARD_SIMILARITY_THRESHOLD) break;

    // require same signal_id -- narrows the pool so unrelated signal types
    // in the same submodule can't collide on shared boilerplate phrasing
    const { data: candSignal } = await supabase
      .from('market_dynamics_signals')
      .select('signal_id')
      .eq('insight_id', candidate.payload.insight_id)
      .limit(1)
      .maybeSingle();

    if (!candSignal || candSignal.signal_id !== signalId) continue;

    console.log(`  [CardMatch] TIER2 score=${candidate.score.toFixed(3)} threshold=${CARD_SIMILARITY_THRESHOLD} card=${candidate.payload.insight_id} signal match confirmed`);

    const { data: card } = await supabase
      .from('market_insights')
      .select('*')
      .eq('id', candidate.payload.insight_id)
      .single();
    if (card) return card;
  }

  console.log(`  [CardMatch] No match in either tier — creating new card`);
  return null;
};

const cleanText = (text) => {
  if (!text) return text;
  return text
    .replace(/^\*+/, '').replace(/\*+$/, '')
    .replace(/^title:\s*/i, '')
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

// Main entry point — called once per relevant Market Dynamics article.
const enrichOrCreateInsight = async (clientId, moduleId, submoduleId, signalId, articleId, articleText, industry, organization) => {
  await setupInsightCentroidCollection();
  const articleEmbedding = await embedText((articleText || '').slice(0, 4000));
  const existing = await findExistingInsight(clientId, moduleId, submoduleId, signalId, articleEmbedding, organization);

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