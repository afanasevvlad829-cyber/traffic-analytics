let DASH = {
  summary: {},
  creative_tasks: [],
  structure: [],
  negatives: { safe: [], blocked: [], actions: [] },
  forecast_review: [],
  action_log: []
};

let CURRENT_SECTION = 'overview';

function esc(x){
  if (x === null || x === undefined) return '';
  return String(x)
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;');
}

async function api(path, options={}){
  const res = await fetch(path, {
    headers: { 'Content-Type':'application/json' },
    ...options
  });
  const text = await res.text();
  let data = {};
  try { data = JSON.parse(text); } catch(e) { data = { raw:text }; }
  if (!res.ok) {
    throw new Error(data.detail || data.error || text || ('HTTP ' + res.status));
  }
  return data;
}

function setSection(name){
  CURRENT_SECTION = name;
  document.querySelectorAll('.nav button').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.section === name);
  });
  document.querySelectorAll('.section').forEach(s => {
    s.classList.toggle('active', s.id === 'section-' + name);
  });
  renderSection();
}

function confidenceBadge(value){
  const v = (value || 'LOW').toUpperCase();
  if (v === 'HIGH') return '<span class="badge good">HIGH</span>';
  if (v === 'MEDIUM') return '<span class="badge warn">MEDIUM</span>';
  return '<span class="badge">LOW</span>';
}

function statusBadge(value){
  const v = (value || '').toUpperCase();
  if (['DONE','EXECUTED','APPROVED'].includes(v)) return `<span class="badge good">${esc(v)}</span>`;
  if (['FAILED','ERROR','NOT_OK'].includes(v)) return `<span class="badge bad">${esc(v)}</span>`;
  if (['PENDING','SNOOZED','IGNORED'].includes(v)) return `<span class="badge warn">${esc(v)}</span>`;
  return `<span class="badge">${esc(v || '-')}</span>`;
}

function bySearch(items, fields, q){
  if (!q) return items;
  const low = q.toLowerCase();
  return items.filter(item =>
    fields.some(f => String(item?.[f] ?? '').toLowerCase().includes(low))
  );
}

function activeFilters(){
  return {
    q: document.getElementById('global-search')?.value?.trim() || '',
    campaign: document.getElementById('campaign-filter')?.value || 'all',
    status: document.getElementById('status-filter')?.value || 'all'
  };
}

function applyBaseFilters(rows){
  const { q, campaign, status } = activeFilters();
  let out = rows.slice();

  if (campaign !== 'all') {
    out = out.filter(r => String(r.campaign_name || r.campaign || '') === campaign);
  }

  if (status !== 'all') {
    out = out.filter(r => String(r.status || r.decision || r.action_status || '').toUpperCase() === status.toUpperCase());
  }

  if (q) {
    out = out.filter(r => JSON.stringify(r).toLowerCase().includes(q.toLowerCase()));
  }

  return out;
}

function renderSummary(){
  const s = DASH.summary || {};
  document.getElementById('summary-cards').innerHTML = `
    <div class="card"><div class="metric-label">Открытые креативы</div><div class="metric-value">${esc(s.open_creatives ?? 0)}</div></div>
    <div class="card"><div class="metric-label">Структурные задачи</div><div class="metric-value">${esc(s.structure_items ?? 0)}</div></div>
    <div class="card"><div class="metric-label">Forecast review</div><div class="metric-value">${esc(s.forecast_items ?? 0)}</div></div>
    <div class="card"><div class="metric-label">Одобрено</div><div class="metric-value">${esc(s.approved_actions ?? 0)}</div></div>
    <div class="card"><div class="metric-label">Pending actions</div><div class="metric-value">${esc(s.pending_actions ?? 0)}</div></div>
  `;
}

function renderCampaignFilter(){
  const campaigns = new Set();
  [...DASH.creative_tasks, ...DASH.structure, ...(DASH.negatives?.safe || []), ...DASH.forecast_review].forEach(r => {
    const c = r.campaign_name || r.campaign;
    if (c) campaigns.add(c);
  });
  const select = document.getElementById('campaign-filter');
  const current = select.value || 'all';
  select.innerHTML = `<option value="all">Все кампании</option>` +
    [...campaigns].sort().map(c => `<option value="${esc(c)}">${esc(c)}</option>`).join('');
  if ([...campaigns].includes(current)) select.value = current;
}

function renderOverview(){
  const weak = applyBaseFilters(DASH.creative_tasks).slice(0, 8);
  const structure = applyBaseFilters(DASH.structure).slice(0, 6);
  const forecast = applyBaseFilters(DASH.forecast_review).slice(0, 6);

  document.getElementById('section-overview').innerHTML = `
    <div class="split">
      <div class="panel">
        <div class="panel-title">Слабые объявления</div>
        ${weak.length ? weak.map(r => `
          <div class="panel" style="padding:12px;margin-bottom:10px">
            <div class="row" style="justify-content:space-between">
              <div>
                <div><b>${esc(r.campaign_name)}</b></div>
                <div class="small muted">Ad ID: ${esc(r.ad_id)} · Group: ${esc(r.ad_group_id)}</div>
              </div>
              ${confidenceBadge(r.prediction_confidence)}
            </div>
            <div class="row" style="margin-top:10px">
              <span class="badge">Score ${esc(r.score)}</span>
              <span class="badge">CTR ${esc(r.ctr_pct)}%</span>
              <span class="badge">Pred CTR ${esc(r.predicted_ctr_pct || '-')}%</span>
              ${statusBadge(r.decision)}
            </div>
            <div style="margin-top:10px" class="small">${esc(r.original_title || '')}</div>
            <div class="row" style="margin-top:10px">
              <button class="btn primary" onclick="queueAB(${r.ad_id}, 'A')">Одобрить A</button>
              <button class="btn" onclick="ignoreCreative(${r.ad_id})">Игнорировать</button>
              <button class="btn warn" onclick="snoozeCreative(${r.ad_id})">Отложить</button>
              <button class="btn ghost" onclick="copyContext('CR-${r.ad_id}')">Скопировать для AI</button>
            </div>
          </div>
        `).join('') : `<div class="empty">Нет слабых объявлений</div>`}
      </div>

      <div>
        <div class="panel">
          <div class="panel-title">Структурные проблемы</div>
          ${structure.length ? structure.map(r => `
            <div class="panel" style="padding:12px;margin-bottom:10px">
              <div><b>${esc(r.campaign_name)}</b></div>
              <div class="small muted">Group: ${esc(r.ad_group_id)}</div>
              <div class="row" style="margin-top:10px">
                ${statusBadge(r.action_status)}
                <button class="btn primary" onclick="structureAction(${JSON.stringify(r.campaign_name)}, ${r.ad_group_id}, 'APPLY_SPLIT')">Apply split</button>
                <button class="btn ghost" onclick="copyContext('ST-${r.ad_group_id}')">Скопировать для AI</button>
              </div>
            </div>
          `).join('') : `<div class="empty">Нет structural items</div>`}
        </div>

        <div class="panel">
          <div class="panel-title">Forecast review</div>
          ${forecast.length ? forecast.map(r => `
            <div class="panel" style="padding:12px;margin-bottom:10px">
              <div><b>${esc(r.campaign_name)}</b></div>
              <div class="small muted">Ad ID: ${esc(r.ad_id)} · ${esc(r.forecast_status || '-')}</div>
              <div class="row" style="margin-top:10px">
                <span class="badge">Pred CTR ${esc(r.predicted_ctr_pct || '-')}</span>
                <span class="badge">Actual CTR ${esc(r.actual_ctr_pct || '-')}</span>
                <span class="badge">Pred CPC ${esc(r.predicted_cpc || '-')}</span>
                <span class="badge">Actual CPC ${esc(r.actual_cpc || '-')}</span>
              </div>
            </div>
          `).join('') : `<div class="empty">Нет forecast review</div>`}
        </div>
      </div>
    </div>
  `;
}

function renderCreatives(){
  const rows = applyBaseFilters(DASH.creative_tasks);
  document.getElementById('section-creatives').innerHTML = `
    <div class="table-wrap">
      <table class="table">
        <thead>
          <tr>
            <th>Кампания</th>
            <th>Ad / Group</th>
            <th>Текущее объявление</th>
            <th>Метрики</th>
            <th>Варианты</th>
            <th>Действия</th>
          </tr>
        </thead>
        <tbody>
          ${rows.length ? rows.map(r => `
            <tr>
              <td>
                <b>${esc(r.campaign_name)}</b><br>
                <span class="small muted">${statusBadge(r.decision)} ${confidenceBadge(r.prediction_confidence)}</span>
              </td>
              <td>
                Ad ID: ${esc(r.ad_id)}<br>
                Group: ${esc(r.ad_group_id)}
              </td>
              <td>
                <div><b>T1:</b> ${esc(r.original_title || '')}</div>
                <div><b>T2:</b> ${esc(r.original_title_2 || '')}</div>
                <div><b>Body:</b> ${esc(r.original_body_text || '')}</div>
                <div class="chips">
                  ${(String(r.sample_queries || '').split('|').map(x=>x.trim()).filter(Boolean).slice(0,8)).map(q => `<span class="chip">${esc(q)}</span>`).join('')}
                </div>
              </td>
              <td>
                <div>Score: <b>${esc(r.score)}</b></div>
                <div>CTR: <b>${esc(r.ctr_pct)}%</b></div>
                <div>CPC: <b>${esc(r.cpc)}</b></div>
                <div>Pred CTR: <b>${esc(r.predicted_ctr_pct || '-')}</b></div>
                <div>Pred CPC: <b>${esc(r.predicted_cpc || '-')}</b></div>
              </td>
              <td>
                <div class="code">A:
${esc(r.ai_title_1 || '')}
${esc(r.ai_title_2 || '')}
${esc(r.ai_body_1 || '')}</div>
                <div class="code" style="margin-top:8px">B:
${esc(r.ai_title_1_b || '')}
${esc(r.ai_title_2_b || '')}
${esc(r.ai_body_2 || '')}</div>
                <div class="code" style="margin-top:8px">C:
${esc(r.ai_title_1_c || '')}
${esc(r.ai_title_2_c || '')}
${esc(r.ai_body_3 || '')}</div>
              </td>
              <td>
                <div class="row">
                  <button class="btn primary" onclick="queueAB(${r.ad_id}, 'A')">A/B A</button>
                  <button class="btn primary" onclick="queueAB(${r.ad_id}, 'B')">A/B B</button>
                  <button class="btn primary" onclick="queueAB(${r.ad_id}, 'C')">A/B C</button>
                  <button class="btn" onclick="ignoreCreative(${r.ad_id})">Игнорировать</button>
                  <button class="btn warn" onclick="snoozeCreative(${r.ad_id})">Отложить</button>
                  <button class="btn ghost" onclick="copyContext('CR-${r.ad_id}')">Скопировать для AI</button>
                </div>
              </td>
            </tr>
          `).join('') : `<tr><td colspan="6"><div class="empty">Нет creative tasks</div></td></tr>`}
        </tbody>
      </table>
    </div>
  `;
}

function renderStructure(){
  const rows = applyBaseFilters(DASH.structure);
  document.getElementById('section-structure').innerHTML = `
    <div class="table-wrap">
      <table class="table">
        <thead>
          <tr>
            <th>Кампания</th>
            <th>Group</th>
            <th>Queries</th>
            <th>Recommendation</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          ${rows.length ? rows.map(r => `
            <tr>
              <td><b>${esc(r.campaign_name)}</b></td>
              <td>${esc(r.ad_group_id)}</td>
              <td><div class="code">${esc(r.queries || '')}</div></td>
              <td><div class="code">${esc(r.recommendation || '')}</div></td>
              <td>
                <div class="row">
                  ${statusBadge(r.action_status)}
                  <button class="btn primary" onclick="structureAction(${JSON.stringify(r.campaign_name)}, ${r.ad_group_id}, 'APPLY_SPLIT')">Apply split</button>
                  <button class="btn" onclick="structureAction(${JSON.stringify(r.campaign_name)}, ${r.ad_group_id}, 'IGNORE')">Игнорировать</button>
                  <button class="btn warn" onclick="structureAction(${JSON.stringify(r.campaign_name)}, ${r.ad_group_id}, 'SNOOZE')">Отложить</button>
                  <button class="btn ghost" onclick="copyContext('ST-${r.ad_group_id}')">Скопировать для AI</button>
                </div>
              </td>
            </tr>
          `).join('') : `<tr><td colspan="5"><div class="empty">Нет structure items</div></td></tr>`}
        </tbody>
      </table>
    </div>
  `;
}

function renderNegatives(){
  const safe = applyBaseFilters(DASH.negatives?.safe || []);
  const blocked = applyBaseFilters(DASH.negatives?.blocked || []);
  const actions = DASH.negatives?.actions || [];
  document.getElementById('section-negatives').innerHTML = `
    <div class="grid-2">
      <div class="panel">
        <div class="panel-title">Safe negatives</div>
        ${safe.length ? safe.map(r => `
          <div class="panel" style="padding:12px;margin-bottom:10px">
            <div><b>${esc(r.campaign_name)}</b></div>
            <div class="small muted">Keywords: ${esc(r.keywords_count)}</div>
            <div class="code" style="margin-top:10px">${esc(r.words || '')}</div>
            <div class="row" style="margin-top:10px">
              <button class="btn primary" onclick="applySafeNegatives(${JSON.stringify(r.campaign_name)})">Apply</button>
              <button class="btn ghost" onclick="copyNegativeContext(${JSON.stringify(r.campaign_name)}, ${JSON.stringify(r.words || '')}, ${JSON.stringify(r.keywords_count || 0)})">Скопировать для AI</button>
            </div>
          </div>
        `).join('') : `<div class="empty">Нет safe negatives</div>`}
      </div>
      <div class="panel">
        <div class="panel-title">Blocked negatives</div>
        ${blocked.length ? blocked.map(r => `
          <div class="panel" style="padding:12px;margin-bottom:10px">
            <div><b>${esc(r.campaign_name)}</b></div>
            <div class="small muted">Keywords: ${esc(r.keywords_count)}</div>
            <div class="code" style="margin-top:10px">${esc(r.words || '')}</div>
          </div>
        `).join('') : `<div class="empty">Нет blocked negatives</div>`}
      </div>
    </div>

    <div class="panel">
      <div class="panel-title">Negative actions log</div>
      <div class="table-wrap">
        <table class="table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Campaign</th>
              <th>Action</th>
              <th>Status</th>
              <th>Words</th>
            </tr>
          </thead>
          <tbody>
            ${actions.length ? actions.map(a => `
              <tr>
                <td>${esc(a.created_at || '')}</td>
                <td>${esc(a.campaign_name || '')}</td>
                <td>${esc(a.action_type || '')}</td>
                <td>${statusBadge(a.status)}</td>
                <td><div class="code">${esc(a.words_text || '')}</div></td>
              </tr>
            `).join('') : `<tr><td colspan="5"><div class="empty">Пока пусто</div></td></tr>`}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function renderForecast(){
  const rows = applyBaseFilters(DASH.forecast_review);
  document.getElementById('section-forecast').innerHTML = `
    <div class="table-wrap">
      <table class="table">
        <thead>
          <tr>
            <th>Кампания</th>
            <th>Ad / Group</th>
            <th>Predicted</th>
            <th>Actual</th>
            <th>Status</th>
            <th>Comment</th>
          </tr>
        </thead>
        <tbody>
          ${rows.length ? rows.map(r => `
            <tr>
              <td><b>${esc(r.campaign_name)}</b></td>
              <td>Ad ${esc(r.ad_id)}<br>Group ${esc(r.ad_group_id)}</td>
              <td>
                CTR: ${esc(r.predicted_ctr_pct || '-')}<br>
                CPC: ${esc(r.predicted_cpc || '-')}<br>
                Relevance: ${esc(r.predicted_relevance || '-')}
              </td>
              <td>
                CTR: ${esc(r.actual_ctr_pct || '-')}<br>
                CPC: ${esc(r.actual_cpc || '-')}<br>
                Relevance: ${esc(r.actual_relevance || '-')}
              </td>
              <td>${statusBadge(r.forecast_status)}</td>
              <td><div class="code">${esc(r.comment || '')}</div></td>
            </tr>
          `).join('') : `<tr><td colspan="6"><div class="empty">Нет forecast review</div></td></tr>`}
        </tbody>
      </table>
    </div>
  `;
}

function renderActions(){
  const rows = applyBaseFilters(DASH.action_log);
  document.getElementById('section-actions').innerHTML = `
    <div class="table-wrap">
      <table class="table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Entity</th>
            <th>Key</th>
            <th>Action</th>
            <th>Status</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody>
          ${rows.length ? rows.map(r => `
            <tr>
              <td>${esc(r.created_at || '')}</td>
              <td>${esc(r.entity_type || '')}</td>
              <td>${esc(r.entity_key || '')}</td>
              <td>${esc(r.action || '')}</td>
              <td>${statusBadge(r.status)}</td>
              <td><div class="code">${esc(r.details || '')}</div></td>
            </tr>
          `).join('') : `<tr><td colspan="6"><div class="empty">Журнал пуст</div></td></tr>`}
        </tbody>
      </table>
    </div>
  `;
}

function renderDiagnostics(){
  document.getElementById('section-diagnostics').innerHTML = `
    <div class="panel">
      <div class="panel-title">Системная диагностика</div>
      <div class="row">
        <button class="btn primary" onclick="runDiagnostic()">Запустить диагностику</button>
        <button class="btn" onclick="copyDiagnostic()">Скопировать отчёт</button>
      </div>
      <div class="small muted" id="diag-meta-admin" style="margin-top:12px"></div>
      <textarea class="big" id="diag-output-admin" readonly></textarea>
    </div>
  `;
}

function renderSection(){
  renderSummary();
  renderCampaignFilter();

  if (CURRENT_SECTION === 'overview') renderOverview();
  if (CURRENT_SECTION === 'creatives') renderCreatives();
  if (CURRENT_SECTION === 'structure') renderStructure();
  if (CURRENT_SECTION === 'negatives') renderNegatives();
  if (CURRENT_SECTION === 'forecast') renderForecast();
  if (CURRENT_SECTION === 'actions') renderActions();
  if (CURRENT_SECTION === 'diagnostics') renderDiagnostics();
}

async function reloadAll(){
  try{
    DASH = await api('/api/full-dashboard');
    renderSection();
  }catch(e){
    document.getElementById('section-overview').innerHTML = `<div class="panel"><div class="panel-title">Ошибка загрузки</div><div class="code">${esc(e.message)}</div></div>`;
  }
}

async function queueAB(adId, variant){
  const data = await api('/api/queue-ab-test', {
    method:'POST',
    body: JSON.stringify({ ad_id: adId, variant })
  });
  alert(data.message || 'Поставлено в очередь');
  await reloadAll();
}

async function ignoreCreative(adId){
  const data = await api('/api/ignore-creative', {
    method:'POST',
    body: JSON.stringify({ ad_id: adId })
  });
  alert(data.message || 'Игнорироватьd');
  await reloadAll();
}

async function snoozeCreative(adId){
  const data = await api('/api/snooze', {
    method:'POST',
    body: JSON.stringify({
      entity_type:'creative',
      entity_key:String(adId),
      days:1,
      reason:'admin ui snooze'
    })
  });
  alert(data.message || 'Отложитьd');
  await reloadAll();
}

async function structureAction(campaign_name, ad_group_id, action){
  const data = await api('/api/structure-action', {
    method:'POST',
    body: JSON.stringify({ campaign_name, ad_group_id, action, reason:'admin ui' })
  });
  alert(data.message || 'Выполнено');
  await reloadAll();
}

async function applySafeNegatives(campaign_name){
  const data = await api('/api/apply-safe-negatives', {
    method:'POST',
    body: JSON.stringify({ campaign_name })
  });
  alert(data.message || 'Поставлено в очередь');
  await reloadAll();
}

async function copyContext(code){
  try{
    const data = await api('/api/context/' + encodeURIComponent(code));
    const payload = data.payload_json || data.payload || data;
    const text = `[AI_CONTEXT]
CODE: ${code}

${JSON.stringify(payload, null, 2)}

QUESTION:
Проанализируй этот объект и дай рекомендации.
[/AI_CONTEXT]`;
    await navigator.clipboard.writeText(text);
    alert('AI context скопирован');
  }catch(e){
    alert('Не удалось получить context: ' + e.message);
  }
}

async function copyNegativeContext(campaign, words, count){
  const text = `[AI_CONTEXT]
TYPE: negative_review
CAMPAIGN: ${campaign}
KEYWORDS_COUNT: ${count}

WORDS:
${words}

QUESTION:
Проанализируй этот набор минус-слов. Какие безопасно применять, какие рискованные?
[/AI_CONTEXT]`;
  await navigator.clipboard.writeText(text);
  alert('Negative context скопирован');
}

async function runDiagnostic(){
  if (CURRENT_SECTION !== 'diagnostics') {
    setSection('diagnostics');
  }
  const output = document.getElementById('diag-output-admin');
  const meta = document.getElementById('diag-meta-admin');
  if (!output || !meta) return;

  output.value = 'Собираю диагностику...';
  meta.textContent = 'running...';

  try{
    const data = await api('/api/diagnostic');
    output.value = data.content || '';
    meta.textContent = `ok=${data.ok} returncode=${data.returncode} report=${data.report_path}`;
  }catch(e){
    output.value = 'Ошибка запуска диагностики: ' + e.message;
    meta.textContent = 'failed';
  }
}

async function copyDiagnostic(){
  const output = document.getElementById('diag-output-admin');
  if (!output) {
    alert('Сначала открой раздел Диагностика и запусти проверку');
    return;
  }
  await navigator.clipboard.writeText(output.value || '');
  alert('Диагностический отчёт скопирован');
}

document.addEventListener('DOMContentLoaded', async () => {
  document.getElementById('global-search').addEventListener('input', renderSection);
  document.getElementById('campaign-filter').addEventListener('change', renderSection);
  document.getElementById('status-filter').addEventListener('change', renderSection);
  await reloadAll();
});
