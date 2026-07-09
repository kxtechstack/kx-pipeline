const { QdrantClient } = require('@qdrant/js-client-rest');
const { pipeline } = require('@xenova/transformers');
const axios = require('axios');
const { ChatPromptTemplate } = require('@langchain/core/prompts');
const { StringOutputParser } = require('@langchain/core/output_parsers');
const { RunnableSequence } = require('@langchain/core/runnables');
const { BaseChatModel } = require('@langchain/core/language_models/chat_models');
const { AIMessage } = require('@langchain/core/messages');
const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const { setupPolicyCollection } = require('./llmRelevanceProcessor'); // CHANGED: new

const qdrant = new QdrantClient({
  url: process.env.QDRANT_URL,
  apiKey: process.env.QDRANT_API_KEY,
  checkCompatibility: false,
});

const POLICY_COLLECTION = process.env.POLICY_QDRANT_COLLECTION || 'policy_articles';

const getRagPromptTemplate = async () => {
  const { data, error } = await supabase
    .from('prompts')
    .select('prompt_template')
    .eq('id', 'rag_chat_v1')
    .eq('is_active', true)
    .single();

  if (error || !data) {
    throw new Error(`Could not load RAG prompt: ${error?.message}`);
  }

  return data.prompt_template;
};

// ── Local embedding model ────────────────────────────────────────────────────
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

// ── LangChain wrapper for LM Studio ─────────────────────────────────────────
class LMStudioChat extends BaseChatModel {
  constructor() {
    super({});
  }

  _llmType() {
    return 'lmstudio';
  }

  async _generate(messages) {
    const formatted = messages.map(m => ({
      role: m._getType() === 'human' ? 'user' : m._getType() === 'system' ? 'system' : 'assistant',
      content: m.content,
    }));

    const response = await axios.post(process.env.LM_STUDIO_URL, {
      model: process.env.LM_STUDIO_MODEL,
      messages: formatted,
      temperature: 0.1,
      max_tokens: 500,
    }, {
      timeout: 180000,
      headers: {
        'Content-Type': 'application/json',
        'ngrok-skip-browser-warning': 'true',
      },
    });

    const content = response.data.choices[0].message.content.trim();
    return {
      generations: [{ message: new AIMessage(content), text: content }],
    };
  }
}

// ── RAG chain using LangChain ────────────────────────────────────────────────
// CHANGED: askQuestion now takes moduleId and filters Qdrant search by it,
// so chat answers on one module's tab don't pull in content from other modules.
const askQuestion = async (question, clientId, industry, moduleId) => {

  await setupPolicyCollection(); // CHANGED: ensures module_id index exists before searching

  // Step 1 — embed question and retrieve from Qdrant
  const questionVector = await embedText(question);

  const searchResults = await qdrant.search(POLICY_COLLECTION, {
    vector: questionVector,
    limit: 5,
    filter: {
      must: [
        { key: 'client_id', match: { value: clientId } },
        { key: 'industry', match: { value: industry } },
        { key: 'module_id', match: { value: moduleId } }, // CHANGED: new
      ],
    },
    with_payload: true,
  });
    const filteredResults = searchResults.filter(r => r.score >= 0.50);


  console.log('[RAG] Retrieved chunks:');
  filteredResults.forEach((r, i) => {
    console.log(`[${i+1}] Score: ${r.score.toFixed(3)} | Title: ${r.payload.title}`);
    console.log(`     Chunk: ${r.payload.chunk_text.slice(0, 150)}`);
  });

  if (!filteredResults || filteredResults.length === 0) {
    return { answer: 'No relevant policy information found for your question.', sources: [] };
  }

  // Step 2 — build context
  const context = filteredResults
    .map((r, i) => `[${i + 1}] ${r.payload.title}\n${r.payload.chunk_text}`)
    .join('\n\n');

  // Step 3 — LangChain RAG chain
  const llm = new LMStudioChat();

  const promptTemplate = await getRagPromptTemplate();

  const prompt = ChatPromptTemplate.fromMessages([
  ["system", promptTemplate]
]);

  const chain = RunnableSequence.from([
    prompt,
    llm,
    new StringOutputParser(),
  ]);

  const answer = await chain.invoke({
    context,
    question,
    industry
});

  let cleanedAnswer = answer
  .replace(/^According to the (provided )?policy articles[:,-]?\s*/i, "")
  .replace(/^According to the articles[:,-]?\s*/i, "")
  .replace(/the articles state that\s*/gi, "")
  .replace(/Based on the retrieved context[:,-]?\s*/gi, "")
  .replace(/\*\*(.*?)\*\*/g, '$1')
  .replace(/\*(.*?)\*/g, '$1')
  .replace(/#{1,6}\s/g, '')
  .replace(/^\s*[-*]\s/gm, '• ')
  .replace(/\n{3,}/g, '\n\n')
  .trim();


  // Step 4 — deduplicate sources
  const sources = [...new Map(filteredResults.map(r => [r.payload.url, {
    title: r.payload.title,
    url: r.payload.url,
  }])).values()];

  return { answer: cleanedAnswer, sources };

};

module.exports = { askQuestion };