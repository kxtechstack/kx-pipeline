// test-client-context.js
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const getClientContext = async (clientId) => {
  const { data } = await supabase
    .schema('admin')
    .from('client_icp')
    .select('context_json')
    .eq('client_id', clientId)
    .single();

  const ctx = data.context_json;
  const lines = [];
  if (ctx.competitors) lines.push(`Known competitors: ${ctx.competitors.join(', ')}`);
  if (ctx.core_sectors) lines.push(`Core sectors: ${ctx.core_sectors.join(', ')}`);
  if (ctx.focus_products_services) lines.push(`Focus products/services: ${ctx.focus_products_services.join(', ')}`);
  if (ctx.geographic_focus) lines.push(`Geographic focus: ${ctx.geographic_focus.join(', ')}`);
  if (ctx.sectors_to_avoid) lines.push(`Lower priority / not core focus for this client: ${ctx.sectors_to_avoid.join(', ')}`);
  return lines.join('\n');
};

getClientContext('b61b4d3b-caeb-457b-9971-636c83688ee4').then(console.log);