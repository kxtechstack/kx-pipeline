// Matches the NEW marketInsights.js exactly:
// TIER 1 — exact organization match (deterministic, no embedding)
// TIER 2 — embedding match (top-5 candidates) but ONLY counts if the
//          candidate card shares the SAME signal_id, threshold 0.82
// Centroid frozen to first 3 founding members.

const { pipeline } = require('@xenova/transformers');

const CARD_SIMILARITY_THRESHOLD = 0.82;

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

const mean = (vectors) => {
  const dim = vectors[0].length;
  const out = new Array(dim).fill(0);
  for (const v of vectors) for (let i = 0; i < dim; i++) out[i] += v[i];
  for (let i = 0; i < dim; i++) out[i] /= vectors.length;
  return out;
};

let seedState = 7;
const rand = () => {
  seedState = (seedState * 1103515245 + 12345) & 0x7fffffff;
  return seedState / 0x7fffffff;
};
const pick = (arr) => arr[Math.floor(rand() * arr.length)];
const shuffleArr = (arr) => {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
};

const companies = [
  'Remedy', 'Dolce Glow', 'Inglot', 'Merit', 'Saltair', 'Be Clinical', 'CiFLAVORS', 'Diamond Wipes',
  'Hello Klean', 'Rael', 'Phitku', 'Yepoda', 'Tarte Cosmetics', 'Beekman 1802', 'Ulta Beauty',
  'Noli', 'Cosmecca Korea', 'DOUGLAS Group', 'COSMAX', 'AmorePacific', 'Grupo Boticário', 'Natura&Co',
  'L\u2019Or\u00e9al', 'Sephora', 'LVMH', 'Estee Lauder', 'Shiseido', 'Kao Corp', 'Coty', 'Revlon',
  'Glossier', 'Fenty Beauty', 'Rare Beauty', 'Kylie Cosmetics', 'Charlotte Tilbury', 'Drunk Elephant',
  'Youth To The People', 'Summer Fridays', 'Tower 28', 'Ilia Beauty', 'Kosas',
  'Vacation Inc', 'Topicals', 'Pattern Beauty', 'K18 Hair', 'Olaplex',
  'Living Proof', 'Function of Beauty', 'Prose', 'Curology', 'Nutrafol', 'Ouai Haircare',
];
const investors = ['L Catterton', 'CAVU Consumer Partners', 'TSG Consumer Partners', 'Avallon', 'KKR', 'River Associates', 'General Atlantic', 'Advent International'];

const investmentTemplates = [
  (c, i, amt) => `${c} closed a $${amt} million Series A led by ${i}. The capital will fund inventory, retail distribution, product innovation and team expansion.`,
  (c, i, amt) => `${i} has led a $${amt} million investment into ${c}, aiming to accelerate clinical research and retail distribution.`,
  (c, i, amt) => `${c} raised $${amt} million in growth funding backed by ${i}, reshaping product, supply, and go-to-market strategy.`,
];
const aiTemplates = [
  (c) => `${c} is deploying generative AI across its marketing and product functions to cut turnaround time and improve personalization.`,
  (c) => `${c}'s new AI-powered tool accelerates R&D timelines, turning weeks of research into minutes.`,
  (c) => `${c} partners with an AI vendor to build conversational try-on and product discovery tools for consumers.`,
];
// Each TREND cluster gets its OWN signal_id -- models the classifier having
// identified it as a specific, narrowly-scoped monitored signal (not the
// generic "Funding Round" / "AI Tool Adoption" signal that SAMECO/SINGLE use).
const trendTemplates = {
  'Investment Activity': [
    (c) => `${c} is among a wave of dermocosmetics brands attracting venture capital as investors chase clinical, evidence-backed skincare.`,
    (c) => `Investors are pouring capital into ${c} and similar evidence-based skincare brands, betting on the dermocosmetics category's growth.`,
    (c) => `${c}'s recent funding round reflects a broader investor pivot toward clinically-backed, dermatologist-led beauty brands.`,
  ],
  'AI Adoption': [
    (c) => `${c} is rolling out AI-powered virtual try-on tools as beauty retailers race to modernize the online shopping experience.`,
    (c) => `Beauty brands including ${c} are adopting AI-driven try-on technology to reduce returns and boost online conversion.`,
    (c) => `${c}'s new virtual try-on feature is part of a wider industry shift toward AI-powered shopping tools in beauty retail.`,
  ],
};

// Fixed "main" signal_ids -- the generic monitored signal that funding-round
// and AI-tool-adoption articles get classified under, REGARDLESS of company.
// This is the realistic risk case: SAMECO and SINGLE share this signal_id,
// so Tier 2 will let them compete against each other if embedding is high enough.
const MAIN_SIGNAL = { 'Investment Activity': 'signal-funding-round', 'AI Adoption': 'signal-ai-tool-adoption' };

const usedCompanies = shuffleArr(companies);
let companyIdx = 0;
const nextCompany = () => usedCompanies[companyIdx++ % usedCompanies.length];

const signals = [];

// 10 SAME-COMPANY duplicate clusters — MUST end up on 1 card each (Tier 1 guarantees this)
for (let k = 0; k < 10; k++) {
  const submodule = k % 2 === 0 ? 'Investment Activity' : 'AI Adoption';
  const company = nextCompany();
  const clusterSize = 2 + (k % 2);
  const signalId = MAIN_SIGNAL[submodule];
  if (submodule === 'Investment Activity') {
    const investor = pick(investors);
    const amt = 5 + Math.floor(rand() * 45);
    for (let v = 0; v < clusterSize; v++) {
      const text = investmentTemplates[v % investmentTemplates.length](company, investor, amt);
      signals.push({ group: `SAMECO-${k}`, organization: company, submodule, signalId, title: text.slice(0, 60), text });
    }
  } else {
    for (let v = 0; v < clusterSize; v++) {
      const text = aiTemplates[v % aiTemplates.length](company);
      signals.push({ group: `SAMECO-${k}`, organization: company, submodule, signalId, title: text.slice(0, 60), text });
    }
  }
}

// 10 SAME-TOPIC-DIFFERENT-COMPANY clusters, each its OWN dedicated signal_id
for (let k = 0; k < 10; k++) {
  const submodule = k % 2 === 0 ? 'Investment Activity' : 'AI Adoption';
  const clusterSize = 2 + (k % 2);
  const templates = trendTemplates[submodule];
  const signalId = `signal-trend-${submodule}-${k}`;
  for (let v = 0; v < clusterSize; v++) {
    const company = nextCompany();
    const text = templates[v % templates.length](company);
    signals.push({ group: `TREND-${k}`, organization: company, submodule, signalId, title: text.slice(0, 60), text });
  }
}

// ~60 unrelated singleton events, using the MAIN signal_id (same as SAMECO) —
// this is the realistic stress case for Tier 2's signal-scoping + threshold.
const singletonSubmodules = ['Investment Activity', 'AI Adoption'];
for (let s = 0; s < 60; s++) {
  const submodule = singletonSubmodules[s % singletonSubmodules.length];
  const company = nextCompany();
  const signalId = MAIN_SIGNAL[submodule];
  let text;
  if (submodule === 'Investment Activity') {
    const investor = pick(investors);
    const amt = 3 + Math.floor(rand() * 60);
    text = investmentTemplates[Math.floor(rand() * investmentTemplates.length)](company, investor, amt);
  } else {
    text = aiTemplates[Math.floor(rand() * aiTemplates.length)](company);
  }
  signals.push({ group: `SINGLE-${s}`, organization: company, submodule, signalId, title: text.slice(0, 60), text });
}

const ordered = shuffleArr(signals);
console.log(`Total signals to process: ${ordered.length}\n`);

(async () => {
  const cardsBySubmodule = {};
  let nextCardId = 1;
  const log = [];
  const orgLastCard = {}; // "submodule::organization" -> most recent card (Tier 1 lookup)

  for (const sig of ordered) {
    const vec = await embedText(sig.text.slice(0, 4000));
    const bucket = (cardsBySubmodule[sig.submodule] ||= []);
    const orgKey = `${sig.submodule}::${sig.organization}`;

    const orgSignalKey = `${sig.submodule}::${sig.organization}::${sig.signalId}`;

    let chosen = null;
    let matchType = null;
    let score = null;

    // TIER 1 — exact org match, scoped to the same signal_id
    if (sig.organization && orgLastCard[orgSignalKey]) {
      chosen = orgLastCard[orgSignalKey];
      matchType = 'TIER1-org';
    }

    // TIER 2 — top-5 embedding candidates, must share signal_id, threshold 0.82
    if (!chosen) {
      const ranked = bucket
        .map(card => ({ card, s: cosineSimilarity(vec, card.centroid) }))
        .sort((a, b) => b.s - a.s)
        .slice(0, 5);

      for (const cand of ranked) {
        if (cand.s < CARD_SIMILARITY_THRESHOLD) break;
        if (cand.card.signalId !== sig.signalId) continue; // signal-scoping gate
        chosen = cand.card;
        matchType = 'TIER2-embedding';
        score = cand.s;
        break;
      }
    }

    if (chosen) {
      chosen.members.push({ title: sig.title, group: sig.group, organization: sig.organization, vec });
      // FROZEN: centroid only from first 3 founding members
      chosen.centroid = mean(chosen.members.slice(0, 3).map(m => m.vec));
      log.push({ title: sig.title, group: sig.group, org: sig.organization, action: 'MERGED', cardId: chosen.id, score, matchType });
    } else {
      const card = { id: nextCardId++, signalId: sig.signalId, members: [{ title: sig.title, group: sig.group, organization: sig.organization, vec }], centroid: vec };
      bucket.push(card);
      log.push({ title: sig.title, group: sig.group, org: sig.organization, action: 'NEW CARD', cardId: card.id, score: null, matchType: null });
    }

    orgLastCard[orgSignalKey] = chosen || bucket[bucket.length - 1];
  }

  console.log('=== DECISION LOG ===\n');
  for (const l of log) {
    const scoreStr = l.score !== null ? l.score.toFixed(3) : '  —  ';
    const mt = l.matchType ? ` [${l.matchType}]` : '';
    console.log(`[score ${scoreStr}] ${l.action.padEnd(9)} card#${l.cardId}  org="${l.org}"  "${l.title}"${mt}  (group: ${l.group})`);
  }

  console.log('\n=== FINAL CARDS ===\n');
  for (const [submodule, cards] of Object.entries(cardsBySubmodule)) {
    console.log(`--- ${submodule} (${cards.length} cards) ---`);
    for (const card of cards) {
      const orgs = new Set(card.members.map(m => m.organization));
      console.log(`  Card #${card.id} (${card.members.length} signal${card.members.length > 1 ? 's' : ''}) signalId=${card.signalId} orgs=[${[...orgs].join(', ')}]`);
      for (const m of card.members) console.log(`     - [${m.group}] org=${m.organization} "${m.title}"`);
    }
  }

  const byGroup = {};
  for (const sig of ordered) (byGroup[sig.group] ||= []).push(sig);

  console.log('\n=== INTENT CHECK ===');
  let pass = 0, fail = 0;
  for (const [group, sigs] of Object.entries(byGroup)) {
    if (group.startsWith('SINGLE')) continue;
    const cardIdsUsed = new Set();
    for (const submodule of Object.values(cardsBySubmodule)) {
      for (const card of submodule) {
        if (card.members.some(m => m.group === group)) cardIdsUsed.add(card.id);
      }
    }
    const label = group.startsWith('SAMECO') ? 'SAME-COMPANY' : 'SAME-TOPIC-DIFF-COMPANY';
    const ok = cardIdsUsed.size === 1;
    ok ? pass++ : fail++;
    console.log(`${group} [${label}]: ${ok ? '✅ merged correctly' : `❌ SPLIT across ${cardIdsUsed.size} cards`} — cards: ${[...cardIdsUsed].join(',')}`);
  }

  let wrongMerges = 0;
  for (const submodule of Object.values(cardsBySubmodule)) {
    for (const card of submodule) {
      const groups = new Set(card.members.map(m => m.group.split('-')[0]));
      const distinctRealGroups = new Set(card.members.map(m => m.group));
      // flag if it mixes signals from genuinely different "real" clusters/singles
      if (distinctRealGroups.size > 1 && (groups.size > 1 || card.members.length !== [...new Set(card.members.map(m=>m.group))].length)) {
        // more precise: flag if members come from more than one distinct group AND
        // those groups aren't all the same SAMECO-N (which is correct)
        const uniqueGroups = [...distinctRealGroups];
        const allSameCluster = uniqueGroups.every(g => g === uniqueGroups[0]);
        if (!allSameCluster) {
          wrongMerges++;
          console.log(`⚠ Card #${card.id} mixes unrelated groups: ${uniqueGroups.join(', ')}`);
        }
      }
    }
  }

  console.log(`\n=== SUMMARY: ${pass} clusters correctly merged, ${fail} incorrectly split, ${wrongMerges} cards with unrelated signals mixed in ===`);
})();