const { QdrantClient } = require('@qdrant/js-client-rest');
const { pipeline } = require('@xenova/transformers');
const axios = require('axios');

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';

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

const askQuestion = async (question, clientId, industry) => {

  // Step 1 — embed the question
  const questionVector = await embedText(question);

  // Step 2 — search Qdrant for relevant chunks
  const searchResults = await qdrant.search(POLICY_COLLECTION, {
    vector: questionVector,
    limit: 5,
    filter: {
      must: [
        { key: 'client_id', match: { value: clientId } },
        { key: 'industry', match: { value: industry } },
      ],
    },
    with_payload: true,
  });

  if (!searchResults || searchResults.length === 0) {
    return {
      answer: 'No relevant policy information found for your question.',
      sources: []
    };
  }

  // Step 3 — build context from retrieved chunks
  const context = searchResults
    .map((r, i) => `[${i + 1}] ${r.payload.title}\n${r.payload.chunk_text}`)
    .join('\n\n');

  // Step 4 — call LM Studio
  const prompt = `You are a policy and regulatory intelligence assistant. Answer the user's question using ONLY the policy articles provided below. If the answer is not in the articles, say "I don't have enough information to answer that from the available policy data."

POLICY ARTICLES:
${context}

USER QUESTION:
${question}

Answer clearly and concisely based only on the above articles.`;

  const response = await axios.post(process.env.LM_STUDIO_URL, {
    model: process.env.LM_STUDIO_MODEL,
    messages: [
      { role: 'system', content: 'You are a strict policy intelligence assistant. Only answer based on the provided context. Never make up information.' },
      { role: 'user', content: prompt },
    ],
    temperature: 0.1,
    max_tokens: 500,
  }, {
    timeout: 180000,
    headers: {
      'Content-Type': 'application/json',
      'ngrok-skip-browser-warning': 'true',
    },
  });

  const answer = response.data.choices[0].message.content.trim();

  // Step 5 — deduplicate sources
  const sources = [...new Map(searchResults.map(r => [r.payload.url, {
    title: r.payload.title,
    url: r.payload.url,
  }])).values()];

  return { answer, sources };
};

module.exports = { askQuestion };