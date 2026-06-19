/**
 * qualityFilter.js
 * ==================
 * Lightweight pre-LLM quality filter. Runs AFTER dedup, BEFORE any LLM
 * relevance call, so we never spend LLM cost/time on content that
 * wouldn't add value anyway.
 *
 * Rules (sourced from pipeline_config table in Supabase):
 *   1. Minimum content length  -> default 200 characters
 *   2. Language                -> English only
 *   3. Article freshness       -> published within last N days (default 30)
 *
 * Paywalled articles are handled automatically by rule #1 — Exa can only
 * fetch a short snippet (usually under 150 chars) from paywalled pages,
 * so they get caught by the length filter at zero extra cost. No special
 * paywall-detection logic needed.
 */

const { franc } = require('franc-min');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const CONFIG_TABLE = 'pipeline_config';

// Fallback defaults if the config table is empty or unreachable —
// pipeline should never hard-fail just because config couldn't load.
const DEFAULT_CONFIG = {
  min_content_length: 200,
  language: 'english',
  freshness_days: 30,
};

// ── Load quality rules from Supabase (cached per process run) ───────────────
let cachedConfig = null;

const loadConfig = async () => {
  if (cachedConfig) return cachedConfig;

  try {
    const { data, error } = await supabase
      .from(CONFIG_TABLE)
      .select('key, value');

    if (error || !data || data.length === 0) {
      console.log('[QualityFilter] No config found in DB, using defaults.');
      cachedConfig = DEFAULT_CONFIG;
      return cachedConfig;
    }

    const config = { ...DEFAULT_CONFIG };
    for (const row of data) {
      if (row.key === 'min_content_length') config.min_content_length = Number(row.value);
      if (row.key === 'language') config.language = row.value;
      if (row.key === 'freshness_days') config.freshness_days = Number(row.value);
    }

    cachedConfig = config;
    console.log('[QualityFilter] Loaded config from DB:', config);
    return cachedConfig;

  } catch (err) {
    console.log('[QualityFilter] Error loading config, using defaults:', err.message);
    cachedConfig = DEFAULT_CONFIG;
    return cachedConfig;
  }
};

// ── Individual rule checks ───────────────────────────────────────────────────

const checkMinLength = (article, config) => {
  const length = (article.text || '').trim().length;
  const passed = length >= config.min_content_length;
  return { passed, reason: passed ? null : `too short (${length} chars, need ${config.min_content_length}+)` };
};

const checkLanguage = (article, config) => {
  if (config.language !== 'english') return { passed: true, reason: null }; // rule disabled

  const text = (article.text || article.title || '').trim();
  if (text.length < 10) {
    // Too short to reliably detect — let the length filter catch it instead
    return { passed: true, reason: null };
  }

  const detected = franc(text); // returns ISO 639-3 code, e.g. 'eng', 'fra', 'und' (undetermined)
  const passed = detected === 'eng' || detected === 'und';
  // 'und' (undetermined) is allowed through rather than rejected, since
  // very short or mixed text can confuse the detector — we don't want
  // false rejections of genuinely English articles.
  return { passed, reason: passed ? null : `non-English content detected (${detected})` };
};

const checkFreshness = (article, config) => {
  if (!article.publishedDate) {
    // No date available -> can't verify freshness, let it pass rather than
    // silently dropping potentially-good articles due to missing metadata
    return { passed: true, reason: null };
  }

  const publishedTs = new Date(article.publishedDate).getTime();
  const cutoffTs = Date.now() - config.freshness_days * 24 * 60 * 60 * 1000;
  const passed = publishedTs >= cutoffTs;
  return { passed, reason: passed ? null : `too old (published before ${config.freshness_days}-day window)` };
};

// ── Main filter function ─────────────────────────────────────────────────────
/**
 * @param {Array} articles - articles that already passed dedup checks
 * @returns {Array} articles that passed all quality rules
 */
const filterLowQualityArticles = async (articles) => {
  if (!articles || articles.length === 0) return [];

  const config = await loadConfig();
  const passedArticles = [];

  for (const article of articles) {
    const checks = [
      checkMinLength(article, config),
      checkLanguage(article, config),
      checkFreshness(article, config),
    ];

    const failedCheck = checks.find(c => !c.passed);

    if (failedCheck) {
      console.log(`[QualityFilter] REJECTED: "${article.title}" | ${failedCheck.reason}`);
      continue;
    }

    passedArticles.push(article);
  }

  console.log(`[QualityFilter] ${passedArticles.length} passed out of ${articles.length}`);
  return passedArticles;
};

module.exports = {
  filterLowQualityArticles,
};