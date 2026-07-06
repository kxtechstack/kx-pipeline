/**
 * customSourceExtractor.js
 * ==========================
 * Extracts RAW text from a custom data source, based on its source_type.
 * This is step 1 only -- just "get the text out." No synthesis, no
 * chunking, no embedding happens here (that's customSourceProcessor.js).
 *
 * Returns: { title, text } for every source type, so the processor
 * always gets the same shape regardless of where the content came from.
 */

const cheerio = require('cheerio');
const { UnstructuredClient } = require('unstructured-client');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const unstructuredClient = new UnstructuredClient({
  security: {
    apiKeyAuth: process.env.UNSTRUCTURED_API_KEY,
  },
});

const STORAGE_BUCKET = 'custom-source-files';

// ---------- PLAIN TEXT ----------
// Source already has the text stored directly in the DB row -- nothing to fetch.
const extractFromText = async (source) => {
  return {
    title: source.source_name,
    text: source.text_content || '',
  };
};

// ---------- WEBSITE ----------
// Fetch the page HTML, strip out non-content elements, return visible text.
const extractFromWebsite = async (source) => {
  const url = source.url_or_path;
  if (!url) throw new Error('No url_or_path set for this website source');

  const response = await fetch(url, {
    headers: { 'User-Agent': 'Mozilla/5.0 (compatible; KXBot/1.0)' },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch website: ${response.status} ${response.statusText}`);
  }

  const html = await response.text();
  const $ = cheerio.load(html);

  // Strip elements that are never real content
  $('script, style, nav, header, footer, iframe, noscript, svg').remove();

  const pageTitle = $('title').text().trim() || source.source_name;
  const bodyText = $('body').text().replace(/\s{2,}/g, ' ').trim();

  if (!bodyText || bodyText.length < 50) {
    throw new Error('Website returned little or no readable text (page may require JavaScript)');
  }

  return { title: pageTitle, text: bodyText };
};

// ---------- FILE (PDF, Word, Excel, etc. via link OR upload) ----------
// Uses unstructured.io to pull clean text out of any document type.
const extractFromFile = async (source) => {
  let fileBuffer;
  let fileName;

  if (source.storage_path) {
    // Uploaded file -- pull bytes from Supabase Storage
    const { data, error } = await supabase.storage
      .from(STORAGE_BUCKET)
      .download(source.storage_path);

    if (error || !data) {
      throw new Error(`Failed to download file from storage: ${error?.message || 'no data returned'}`);
    }

    fileBuffer = Buffer.from(await data.arrayBuffer());
    fileName = source.storage_path.split('/').pop();

  } else if (source.url_or_path) {
    // File given as a link (e.g. PDF link) -- fetch it
    const response = await fetch(source.url_or_path);
    if (!response.ok) {
      throw new Error(`Failed to fetch file from URL: ${response.status} ${response.statusText}`);
    }
    fileBuffer = Buffer.from(await response.arrayBuffer());
    fileName = source.url_or_path.split('/').pop().split('?')[0] || 'document';

  } else {
    throw new Error('No storage_path or url_or_path set for this file source');
  }

  const result = await unstructuredClient.general.partition({
    partitionParameters: {
      files: {
        content: fileBuffer,
        fileName: fileName,
      },
      strategy: 'auto',
    },
  });

  // unstructured.io's SDK returns the elements array directly (not wrapped
  // in a .elements property) -- handle both shapes just in case.
  const elements = Array.isArray(result) ? result : (result.elements || []);
  const text = elements.map(el => el.text || '').filter(Boolean).join('\n\n');

  if (!text || text.length < 20) {
    throw new Error('Unstructured.io returned little or no text for this file');
  }

  return { title: source.source_name, text };
};

// ---------- REGISTRY ----------
const extractors = {
  text: extractFromText,
  website: extractFromWebsite,
  pdf: extractFromFile,
  file: extractFromFile, // for when AI Studio renames "PDF Upload" to generic "Upload File"
};

const extractContent = async (source) => {
  const extractor = extractors[source.source_type];
  if (!extractor) {
    throw new Error(`Unknown source_type: "${source.source_type}". Expected one of: ${Object.keys(extractors).join(', ')}`);
  }
  return extractor(source);
};

module.exports = { extractContent, extractFromText, extractFromWebsite, extractFromFile };