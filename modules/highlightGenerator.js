const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const generateHighlight = async (clientId) => {

  const { data: latestSignal } = await supabase
  .from('policy_signals')
  .select('source_published_date')
  .eq('client_id', clientId)
  .order('source_published_date', { ascending: false })
  .limit(1)
  .single();

  if (!latestSignal) {
  console.log('No signals found');
  return;
}

const latestDate =
  latestSignal.source_published_date.split('T')[0];

  const { data: signals, error } = await supabase
  .from('policy_signals')
  .select(`
    signal_title,
    category,
    impact_level,
    country,
    summary,
    business_impact
  `)
  .eq('client_id', clientId)
  .gte('source_published_date', `${latestDate}T00:00:00`)
  .lt('source_published_date', `${latestDate}T23:59:59`);

  if (error) {
    console.error('Error fetching signals:', error);
    return;
  }

  console.log(`Found ${signals.length} signals for highlight generation`);
  const { data: promptRow, error: promptError } = await supabase
  .from('relevance_check_prompts')
  .select('prompt_template')
  .eq('id', 'daily_highlight_v1')
  .eq('is_active', true)
  .single();

if (promptError || !promptRow) {
  console.error('Could not load highlight prompt');
  return;
}

const signalContext = signals.map(signal => `
Signal Title: ${signal.signal_title}

Category: ${signal.category}

Impact: ${signal.impact_level}

Country: ${signal.country}

Summary: ${signal.summary}

Business Impact: ${
  Array.isArray(signal.business_impact)
    ? signal.business_impact.join(', ')
    : signal.business_impact || ''
}
`).join('\n\n-----------------\n\n');

const finalPrompt = `
${promptRow.prompt_template}

POLICY SIGNALS:

${signalContext}
`;

let highlightText;

try {
  const response = await axios.post(
    process.env.LM_STUDIO_URL,
    {
      model: process.env.LM_STUDIO_MODEL,
      messages: [
        {
          role: 'user',
          content: finalPrompt
        }
      ],
      temperature: 0.2,
      max_tokens: 300
    },
    {
      timeout: 100000,
      headers: {
        "Content-Type": "application/json",
        "ngrok-skip-browser-warning": "true"
      }
    }
  );

  highlightText =
    response.data.choices[0].message.content.trim();

} catch (err) {
  console.error('Highlight generation failed:', err.message);
  return;
}

  await supabase
  .from('daily_highlights')
  .delete()
  .eq('client_id', clientId);

  const { error: insertError } = await supabase
    .from('daily_highlights')
    .insert({
      client_id: clientId,
      highlight_text: highlightText
    });

  if (insertError) {
    console.error('Highlight insert error:', insertError);
  } else {
    console.log('Highlight saved');
  }
};

module.exports = {
  generateHighlight
};