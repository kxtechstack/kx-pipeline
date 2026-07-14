/**
 * test-gemini.js
 * ===========================
 * Standalone comparison script — runs the SAME relevance prompt, client
 * context, and code-side overrides as llmRelevanceProcessor.js, but calls
 * Gemini 2.5 Flash instead of the local LM Studio (llama-3.2-3b) model.
 *
 * Goal: see whether a stronger model avoids the JSON-formatting failures,
 * timeouts, and Critical/High confusion seen with the local model — without
 * touching the real pipeline or any stored data.
 *
 * Requires GEMINI_API_KEY in your .env file.
 */

require('dotenv').config();
const axios = require('axios');
const {
  getRelevancePromptTemplate,
  getClientContext,
  fillPromptTemplate,
  getSectorsToAvoid,
  getCompetitorsList,
  applySectorsToAvoidOverride,
  applyCriticalRequiresCompetitorOverride,
} = require('./modules/llmRelevanceProcessor');

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = 'gemini-2.5-flash';
const GEMINI_URL = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`;

if (!GEMINI_API_KEY) {
  console.error('Missing GEMINI_API_KEY in .env — aborting.');
  process.exit(1);
}

// Same test client/module as your earlier test.js
const CLIENT_ID = 'b61b4d3b-caeb-457b-9971-636c83688ee4'; // Lumière Beauty Group
const MODULE_ID = '55c5ee19-bfca-468b-81b3-b89ca4f303c8'; // Market Dynamics
const INDUSTRY = 'Cosmetics';

// Broader set of 7 test cases covering a wider spread of impact levels/scenarios
const testArticles = [
  {
    label: 'Named competitor (Lancôme) acquisition — expect CRITICAL',
    article: {
      title: 'Lancôme Acquires Korean Skincare Tech Startup for $85M',
      text: 'Lancôme, the prestige skincare and cosmetics brand, announced today it has acquired a Korean skincare technology startup for $85 million, aiming to bolster its R&D capabilities in advanced skincare formulation and personalized beauty tech.',
    },
  },
  {
    label: 'Named competitor (Estée Lauder) product recall — expect CRITICAL',
    article: {
      title: 'Estée Lauder Issues Voluntary Recall of Anti-Aging Serum Line',
      text: 'Estée Lauder announced a voluntary recall of its flagship anti-aging serum line across North America and Europe after routine quality testing identified a manufacturing defect. The company said the recall could affect its prestige skincare revenue in the current quarter.',
    },
  },
  {
    label: 'Generic prestige skincare startup funding, no named competitor — expect HIGH',
    article: {
      title: 'Prestige Skincare Startup Glow Labs Raises $30M Series B',
      text: 'Glow Labs, a prestige skincare startup focused on anti-aging treatments, announced a $30 million Series B funding round led by a consumer-focused venture fund, to expand product development and retail distribution.',
    },
  },
  {
    label: 'Geography match (Singapore) prestige retail expansion, no named competitor — expect HIGH',
    article: {
      title: 'Prestige Beauty Retailer Radiance Group Opens 12 New Stores Across Singapore',
      text: 'Radiance Group, a specialty prestige beauty retailer, announced plans to open 12 new stores across Singapore over the next year, as part of its Southeast Asia expansion strategy targeting the growing prestige beauty market.',
    },
  },
  {
    label: 'Mass-market beauty brand launch, weak relevance — expect LOW or MEDIUM',
    article: {
      title: 'Budget Beauty Brand ColorPop Launches New Drugstore Makeup Line',
      text: 'ColorPop, a mass-market makeup brand sold primarily through drugstores and discount retailers, launched a new affordable eyeshadow and lipstick line targeting price-sensitive younger consumers in the US market.',
    },
  },
  {
    label: 'Fragrance industry deal (sectors_to_avoid) — expect LOW after override',
    article: {
      title: 'Boutique Fragrance House Launches Niche Perfume Collection in Paris',
      text: 'A boutique fragrance house unveiled a new niche perfume collection at a launch event in Paris, targeting luxury fragrance collectors. The brand plans a limited retail rollout across select European department stores.',
    },
  },
  {
    label: 'Clearly irrelevant industry (unrelated to cosmetics) — expect IRRELEVANT',
    article: {
      title: 'Regional Trucking Company Expands Fleet with 50 New Diesel Trucks',
      text: 'A regional logistics and trucking company announced the purchase of 50 new diesel trucks to expand its freight capacity across the Midwest, citing rising e-commerce delivery demand.',
    },
  },
];

// ── Call Gemini and parse the JSON response ─────────────────────────────────
const classifyWithGemini = async (promptTemplate, industry, article, clientContext) => {
  const prompt = fillPromptTemplate(promptTemplate, industry, article.title, article.text, clientContext);

  try {
    const response = await axios.post(GEMINI_URL, {
      contents: [
        {
          role: 'user',
          parts: [{ text: `You are a strict, precise classification assistant. You only respond with valid JSON, nothing else, no markdown code fences.\n\n${prompt}` }],
        },
      ],
      generationConfig: {
        temperature: 0.1,
        maxOutputTokens: 2000,
        thinkingConfig: {
          thinkingBudget: 0, // disable internal reasoning tokens — classification doesn't need it, and it was eating into maxOutputTokens, causing truncated JSON
        },
      },
    }, {
      timeout: 60000,
      headers: { 'Content-Type': 'application/json' },
    });

    const rawContent = response.data.candidates[0].content.parts[0].text.trim();
    const cleaned = rawContent.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/i, '').trim();

    let parsed;
    try {
      parsed = JSON.parse(cleaned);
    } catch (err) {
      console.log(`      Raw Gemini response was: ${rawContent}`);
      throw err;
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
    console.log(`  [!] Gemini classification failed for "${article.title}": ${err.message}`);
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

// ── Main ─────────────────────────────────────────────────────────────────────
(async () => {
  const promptTemplate = await getRelevancePromptTemplate(MODULE_ID);
  const clientContext = await getClientContext(CLIENT_ID);
  const sectorsToAvoid = await getSectorsToAvoid(CLIENT_ID);
  const competitors = await getCompetitorsList(CLIENT_ID);

  if (clientContext) {
    console.log(`[ClientContext] Loaded context for client ${CLIENT_ID}:\n${clientContext}\n`);
  }

  for (const { label, article } of testArticles) {
    console.log(`=== ${label} ===`);
    console.log(`[Gemini] Classifying: "${article.title}"`);

    let classification = await classifyWithGemini(promptTemplate, INDUSTRY, article, clientContext);
    classification = applySectorsToAvoidOverride(classification, article, sectorsToAvoid);
    classification = applyCriticalRequiresCompetitorOverride(classification, article, competitors);

    if (classification.technical_failure) {
      console.log(`  [!] FAILED | ${classification.reason}`);
    } else if (classification.is_relevant) {
      console.log(`  [✓] RELEVANT | ${classification.category} | ${classification.impact_level} | ${classification.reason}`);
    } else {
      console.log(`  [✗] IRRELEVANT | ${classification.reason}`);
    }
    console.log('');
  }

  console.log('[Gemini test] Done.');
})();