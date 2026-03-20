let DASH = {
  summary: {},
  creative_tasks: [],
  structure: [],
  negatives: { safe: [], blocked: [], actions: [] },
  forecast_review: [],
  action_log: [],
  scoring_summary: {},
  scoring_visitors: [],
  scoring_timeseries: { dates: [], hot: [], warm: [], cold: [], ready: true },
  scoring_audience: { ready: false, gender_age: [], source_mix: [], device_mix: [], note: '' },
  scoring_attribution: { ready: false, status: 'empty', direct_pct: 0, unknown_pct: 0, top_sources: [] },
  scoring_creative_plan: { ready: false, items: [], count: 0, days: 90 },
  scoring_audiences_cohorts: { ready: false, cohorts: [], matrix: [], days: 90 },
  scoring_ad_templates: { ready: false, items: [], count: 0, days: 90, min_audience_size: 1, include_small: true, variants: 3, error: '' },
  scoring_activation_plan: { ready: false, cohorts: [], count: 0, eligible_count: 0, min_audience_size: 100 },
  scoring_activation_reaction: { ready: false, items: [], count: 0, totals: { impressions: 0, clicks: 0, cost: 0 } },
  scoring_meta: { ready: true, count: 0, limit: 100, error: '' }
};

let CURRENT_SECTION = 'overview';
let IS_SCORING_STANDALONE = false;
let SCORING_FILTERS = { segment: 'all', source: '', limit: 100 };
let SCORING_TABLE = null;
let SCORING_TIMESERIES_CHART = null;
let SCORING_DISTRIBUTION_CHART = null;

function normalizeScoringLimit(value){
  const n = Number(value);
  if (![50, 100, 200].includes(n)) return 100;
  return n;
}

function applyInitialRouteState(){
  const path = String(window.location.pathname || '').replace(/\/+$/, '');
  const params = new URLSearchParams(window.location.search || '');
  const section = String(params.get('section') || '').trim().toLowerCase();

  if (path === '/admin/scoring/creatives') {
    CURRENT_SECTION = 'scoring_creatives';
    IS_SCORING_STANDALONE = true;
  }
  if (path === '/admin/scoring/templates') {
    CURRENT_SECTION = 'scoring_templates';
    IS_SCORING_STANDALONE = true;
  }
  if (path === '/admin/scoring') {
    CURRENT_SECTION = 'scoring';
    IS_SCORING_STANDALONE = true;
  }
  if (['overview', 'creatives', 'structure', 'negatives', 'forecast', 'scoring', 'scoring_creatives', 'scoring_templates', 'actions', 'diagnostics'].includes(section)) {
    CURRENT_SECTION = section;
  }

  const segment = String(params.get('segment') || '').trim().toLowerCase();
  if (['hot', 'warm', 'cold', 'all'].includes(segment)) {
    SCORING_FILTERS.segment = segment;
  }

  const source = String(params.get('source') || '').trim();
  if (source) {
    SCORING_FILTERS.source = source;
  }

  if (params.has('limit')) {
    SCORING_FILTERS.limit = normalizeScoringLimit(params.get('limit'));
  }
}

function applyStandaloneScoringLayout(){
  if (!IS_SCORING_STANDALONE) return;
  document.body.classList.add('scoring-standalone');

  const title = document.getElementById('topbar-title');
  const subtitle = document.getElementById('topbar-subtitle');
  const actions = document.getElementById('topbar-actions');
  if (CURRENT_SECTION === 'scoring_creatives') {
    if (title) title.textContent = 'Креативы по сегментам';
    if (subtitle) subtitle.textContent = 'Гипотезы креативов и офферов для горячих, тёплых и холодных сегментов';
  } else if (CURRENT_SECTION === 'scoring_templates') {
    if (title) title.textContent = 'Шаблоны объявлений';
    if (subtitle) subtitle.textContent = 'Вариации объявлений по cohort, с объяснением и привязкой к группе Direct';
  } else {
    if (title) title.textContent = 'Скоринг посетителей';
    if (subtitle) subtitle.textContent = 'Оценка вероятности покупки и рекомендации для маркетинга';
  }
  if (actions) {
    actions.innerHTML = `
      <a class="btn ghost" href="/admin">Открыть обзор</a>
      <a class="btn ghost" href="/admin/scoring">Отчёт скоринга</a>
      <a class="btn ghost" href="/admin/scoring/creatives">Креативы сегментов</a>
      <a class="btn ghost" href="/admin/scoring/templates">Шаблоны объявлений</a>
      <button class="btn" onclick="loadScoringDataAndRender()">Обновить данные</button>
      <button class="btn primary" onclick="rebuildScoring()">Пересчитать скоринг</button>
    `;
  }

  document.querySelectorAll('.nav button').forEach(btn => {
    const section = btn.dataset.section || '';
    if (!['scoring', 'scoring_creatives', 'scoring_templates'].includes(section)) {
      btn.style.display = 'none';
    } else {
      btn.textContent = section === 'scoring'
        ? 'Скоринг'
        : section === 'scoring_creatives'
          ? 'Креативы сегментов'
          : 'Шаблоны объявлений';
    }
  });
}

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
  if (IS_SCORING_STANDALONE && !['scoring', 'scoring_creatives', 'scoring_templates'].includes(name)) {
    return;
  }
  if (CURRENT_SECTION === 'scoring' && name !== 'scoring') {
    destroyScoringVisuals();
  }
  CURRENT_SECTION = name;
  closeScoringDetails();
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

function segmentBadge(segment){
  const v = String(segment || '').toLowerCase();
  if (v === 'hot') return '<span class="badge good">Горячий</span>';
  if (v === 'warm') return '<span class="badge warn">Тёплый</span>';
  if (v === 'cold') return '<span class="badge">Холодный</span>';
  return `<span class="badge">${esc(v || '-')}</span>`;
}

function shortText(value, maxLen=120){
  const s = String(value || '');
  if (s.length <= maxLen) return s;
  return s.slice(0, maxLen - 1) + '…';
}

function numText(value, digits=2){
  if (value === null || value === undefined || value === '') return '-';
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  return n.toFixed(digits);
}

function safeDomId(value){
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '') || 'item';
}

function factorLabel(key){
  const labels = {
    visited_price_page: 'страница цен',
    visited_program_page: 'страница программы',
    visited_booking_page: 'страница бронирования',
    clicked_booking_button: 'клик по бронированию',
    sessions_count_gt_1: 'повторный визит',
    sessions_count_gt_2: '3+ визита',
    total_time_gt_120: 'время > 2 минут',
    total_time_gt_300: 'время > 5 минут',
    pageviews_gte_4: 'просмотры >= 4',
    pageviews_gte_8: 'просмотры >= 8',
    scroll_70: 'скролл 70%',
    return_visitor: 'returning visitor',
    high_intent_source: 'high-intent source',
    bounce_session: 'bounce session'
  };
  return labels[key] || key;
}

function sourceLabel(value){
  const v = String(value || '').trim().toLowerCase();
  const map = {
    yandex_direct: 'Яндекс Директ',
    vk_ads: 'VK Ads',
    direct: 'Прямые заходы',
    organic: 'Органический поиск',
    referral: 'Реферальный трафик',
    social: 'Соцсети',
    messenger: 'Мессенджеры',
    email: 'Email',
    ad: 'Реклама',
    internal: 'Внутренние переходы',
    unknown: 'Не определено',
    '': 'Не определено',
  };
  if (map[v]) return map[v];
  if (v.includes('gmail')) return 'Email (Gmail)';
  if (v.includes('mail')) return 'Email';
  if (v.includes('direct')) return 'Прямые заходы';
  return value || 'Не определено';
}

function deviceLabel(value){
  const raw = String(value || '').trim();
  const v = raw.toLowerCase();
  if (!v || v === 'unknown') return 'Не определено';
  if (v === '1' || v.includes('desktop')) return 'Десктоп';
  if (v === '2' || v.includes('mobile') || v.includes('smartphone')) return 'Мобильный';
  if (v === '3' || v.includes('tablet')) return 'Планшет';
  if (v.includes('tv')) return 'ТВ';
  return raw;
}

function reasonLabel(value){
  const v = String(value || '').trim().toLowerCase();
  const map = {
    booking_intent: 'Сильный сигнал бронирования',
    price_interest: 'Интерес к цене',
    returning_engaged: 'Повторные вовлечённые визиты',
    content_engaged: 'Вовлечённое изучение контента',
    exploratory_low_intent: 'Ознакомительный трафик',
    bounce_like_session: 'Сессия с низкой вовлечённостью',
    unknown: 'Причина не определена',
    '': 'Причина не определена',
  };
  return map[v] || value || 'Причина не определена';
}

function normalizeTimeseries(ts){
  const dates = Array.isArray(ts?.dates) ? ts.dates : [];
  const hot = Array.isArray(ts?.hot) ? ts.hot : [];
  const warm = Array.isArray(ts?.warm) ? ts.warm : [];
  const cold = Array.isArray(ts?.cold) ? ts.cold : [];
  const size = dates.length;

  return {
    ready: ts?.ready !== false,
    dates,
    hot: hot.slice(0, size).map(v => Number(v || 0)),
    warm: warm.slice(0, size).map(v => Number(v || 0)),
    cold: cold.slice(0, size).map(v => Number(v || 0)),
  };
}

function destroyScoringVisuals(){
  if (SCORING_TABLE && typeof SCORING_TABLE.destroy === 'function') {
    SCORING_TABLE.destroy();
  }
  SCORING_TABLE = null;

  if (SCORING_TIMESERIES_CHART && typeof SCORING_TIMESERIES_CHART.destroy === 'function') {
    SCORING_TIMESERIES_CHART.destroy();
  }
  SCORING_TIMESERIES_CHART = null;

  if (SCORING_DISTRIBUTION_CHART && typeof SCORING_DISTRIBUTION_CHART.destroy === 'function') {
    SCORING_DISTRIBUTION_CHART.destroy();
  }
  SCORING_DISTRIBUTION_CHART = null;
}

function initScoringTable(rows){
  const tableEl = document.getElementById('scoring-table');
  if (!tableEl) return;

  if (typeof Tabulator === 'undefined') {
    tableEl.innerHTML = `<div class="empty">Tabulator не загружен. Обновите страницу.</div>`;
    return;
  }

  SCORING_TABLE = new Tabulator(tableEl, {
    data: rows || [],
    layout: 'fitColumns',
    responsiveLayout: 'collapse',
    pagination: false,
    movableColumns: false,
    height: '520px',
    placeholder: 'Нет данных по скорингу',
    initialSort: [{ column: 'scored_at', dir: 'desc' }],
    rowFormatter: row => {
      const d = row.getData() || {};
      const seg = String(d.segment || '').toLowerCase();
      const el = row.getElement();
      el.style.borderLeft = seg === 'hot' ? '4px solid #16a34a' : seg === 'warm' ? '4px solid #d97706' : '4px solid #94a3b8';
      el.style.background = seg === 'hot' ? '#f6fdf7' : seg === 'warm' ? '#fffaf2' : '#f8fafc';
    },
    rowClick: (_e, row) => {
      const d = row.getData() || {};
      openScoringDetails(d.visitor_id);
    },
    columns: [
      { title: 'Посетитель', field: 'visitor_id', minWidth: 180, headerSort: true },
      {
        title: 'Скор',
        field: 'score',
        width: 90,
        hozAlign: 'center',
        sorter: 'number',
        formatter: cell => Number(cell.getValue() || 0).toFixed(3),
      },
      {
        title: 'Сегмент',
        field: 'segment',
        width: 110,
        hozAlign: 'center',
        sorter: 'string',
        headerFilter: 'list',
        headerFilterParams: {
          values: { '': 'Все', hot: 'Горячий', warm: 'Тёплый', cold: 'Холодный' },
        },
        formatter: cell => segmentBadge(cell.getValue()),
      },
      { title: 'Причина', field: 'short_reason', minWidth: 140, sorter: 'string' },
      {
        title: 'Почему такой скор',
        field: 'human_explanation',
        minWidth: 260,
        formatter: cell => esc(shortText(cell.getValue() || '-', 140)),
      },
      {
        title: 'Рекомендация',
        field: 'recommended_action',
        minWidth: 250,
        formatter: cell => esc(shortText(cell.getValue() || '-', 130)),
      },
      {
        title: 'Источник',
        field: 'traffic_source',
        minWidth: 140,
        sorter: 'string',
        headerFilter: 'input',
        formatter: cell => esc(sourceLabel(cell.getValue())),
      },
      {
        title: 'Рассчитан',
        field: 'scored_at',
        minWidth: 170,
        sorter: 'string',
      },
      {
        title: 'Подробнее',
        width: 120,
        hozAlign: 'center',
        headerSort: false,
        formatter: () => '<button class="btn">Открыть</button>',
        cellClick: (_e, cell) => {
          const row = cell.getRow().getData() || {};
          openScoringDetails(row.visitor_id);
        },
      },
    ],
  });
}

function initScoringCharts(){
  const tsCanvas = document.getElementById('scoring-timeseries-chart');
  const distCanvas = document.getElementById('scoring-distribution-chart');
  if (typeof Chart === 'undefined') {
    if (tsCanvas && tsCanvas.parentElement) tsCanvas.parentElement.innerHTML = '<div class="empty">Chart.js не загружен</div>';
    if (distCanvas && distCanvas.parentElement) distCanvas.parentElement.innerHTML = '<div class="empty">Chart.js не загружен</div>';
    return;
  }
  if (!tsCanvas || !distCanvas) return;

  const ts = normalizeTimeseries(DASH.scoring_timeseries || {});
  const tsTotal = [...ts.hot, ...ts.warm, ...ts.cold].reduce((acc, n) => acc + Number(n || 0), 0);
  if (!ts.dates.length || tsTotal === 0) {
    if (tsCanvas.parentElement) {
      tsCanvas.parentElement.innerHTML = '<div class="empty">Нет данных для графика за выбранный период</div>';
    }
  } else {
  SCORING_TIMESERIES_CHART = new Chart(tsCanvas, {
    type: 'line',
    data: {
      labels: ts.dates,
      datasets: [
        { label: 'Горячие', data: ts.hot, borderColor: '#16a34a', backgroundColor: 'rgba(22,163,74,.15)', tension: 0.25 },
        { label: 'Тёплые', data: ts.warm, borderColor: '#d97706', backgroundColor: 'rgba(217,119,6,.15)', tension: 0.25 },
        { label: 'Холодные', data: ts.cold, borderColor: '#94a3b8', backgroundColor: 'rgba(148,163,184,.15)', tension: 0.25 },
      ],
    },
    options: {
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#f5f7fb' } } },
      scales: {
        x: { ticks: { color: '#8d99ae', maxRotation: 0 }, grid: { color: 'rgba(35,40,54,.55)' } },
        y: { ticks: { color: '#8d99ae' }, grid: { color: 'rgba(35,40,54,.55)' }, beginAtZero: true },
      },
    },
  });
  }

  const s = DASH.scoring_summary || {};
  const distData = [Number(s.hot_count || 0), Number(s.warm_count || 0), Number(s.cold_count || 0)];
  const distTotal = distData.reduce((acc, n) => acc + n, 0);
  if (distTotal === 0) {
    if (distCanvas.parentElement) {
      distCanvas.parentElement.innerHTML = '<div class="empty">Нет данных для распределения сегментов</div>';
    }
    return;
  }

  SCORING_DISTRIBUTION_CHART = new Chart(distCanvas, {
    type: 'doughnut',
    data: {
      labels: ['Горячие', 'Тёплые', 'Холодные'],
      datasets: [
        {
          data: distData,
          backgroundColor: ['#16a34a', '#d97706', '#64748b'],
          borderColor: ['#0f172a', '#0f172a', '#0f172a'],
          borderWidth: 1,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#f5f7fb' } } },
    },
  });
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

function renderScoring(){
  destroyScoringVisuals();

  const s = DASH.scoring_summary || {};
  const rows = DASH.scoring_visitors || [];
  const audience = DASH.scoring_audience || {};
  const aq = DASH.scoring_attribution || {};
  const meta = DASH.scoring_meta || {};
  const avg = Number(s.avg_score || 0).toFixed(3);
  const audienceRows = (audience.gender_age || []).slice(0, 12);
  const sourceRows = (audience.source_mix || []).slice(0, 8);
  const deviceRows = (audience.device_mix || []).slice(0, 8);
  const mobileOsRows = (audience.mobile_os_mix || []).slice(0, 8);

  document.getElementById('section-scoring').innerHTML = `
    <div class="grid-2">
      <div class="card"><div class="metric-label">Горячие</div><div class="metric-value">${esc(s.hot_count ?? 0)}</div></div>
      <div class="card"><div class="metric-label">Тёплые</div><div class="metric-value">${esc(s.warm_count ?? 0)}</div></div>
      <div class="card"><div class="metric-label">Холодные</div><div class="metric-value">${esc(s.cold_count ?? 0)}</div></div>
      <div class="card"><div class="metric-label">Средний скор</div><div class="metric-value">${esc(avg)}</div></div>
    </div>

    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Качество атрибуции</div>
        <span class="badge ${aq.status === 'high' ? 'good' : aq.status === 'medium' ? 'warn' : 'bad'}">${esc(aq.status || 'n/a')}</span>
      </div>
      <div class="grid-3">
        <div class="metric-box"><div class="metric-label">% direct</div><div class="metric-value" style="font-size:24px">${esc(Number(aq.direct_pct || 0).toFixed(1))}%</div></div>
        <div class="metric-box"><div class="metric-label">% unknown</div><div class="metric-value" style="font-size:24px">${esc(Number(aq.unknown_pct || 0).toFixed(1))}%</div></div>
        <div class="metric-box"><div class="metric-label">Окно</div><div class="metric-value" style="font-size:24px">${esc(aq.days || 90)}д</div></div>
      </div>
      <div class="small muted" style="margin-top:10px">Топ источников: ${(aq.top_sources || []).map(x => `${sourceLabel(x.source)} (${x.visitors})`).join(' · ') || 'нет данных'}</div>
    </div>

    <div class="scoring-grid">
      <div class="panel">
        <div class="panel-title">Типы посетителей за 90 дней</div>
        <div class="chart-box">
          <canvas id="scoring-timeseries-chart"></canvas>
        </div>
      </div>
      <div class="panel">
        <div class="panel-title">Распределение сегментов</div>
        <div class="chart-box">
          <canvas id="scoring-distribution-chart"></canvas>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Аудитория Метрики (агрегатно)</div>
        <div class="small muted">последние ${esc(audience.days || 90)} дней</div>
      </div>
      ${audience.note ? `<div class="small muted" style="margin-top:8px">${esc(audience.note)}</div>` : ''}
      ${audience.gender_age_error ? `<div class="code" style="margin-top:8px">gender/age недоступны: ${esc(audience.gender_age_error)}</div>` : ''}
      <div class="table-wrap" style="margin-top:10px">
        <table class="table">
          <thead>
            <tr><th>Пол</th><th>Возраст</th><th>Визиты</th><th>Пользователи</th></tr>
          </thead>
          <tbody>
            ${audienceRows.length ? audienceRows.map(r => `
              <tr>
                <td>${esc(r.gender || 'unknown')}</td>
                <td>${esc(r.age_interval || 'unknown')}</td>
                <td>${esc(r.visits || 0)}</td>
                <td>${esc(r.users || 0)}</td>
              </tr>
            `).join('') : `<tr><td colspan="4"><div class="empty">Нет данных gender/age за выбранный период</div></td></tr>`}
          </tbody>
        </table>
      </div>
      <div class="grid-2">
        <div class="card">
          <div class="metric-label">Топ source (scoring)</div>
          <div class="code">${sourceRows.length ? sourceRows.map(r => `${sourceLabel(r.source)}: ${r.visitors || 0}`).join('\n') : 'нет данных'}</div>
        </div>
        <div class="card">
          <div class="metric-label">Топ устройств</div>
          <div class="code">${deviceRows.length ? deviceRows.map(r => `${deviceLabel(r.device_type)}: ${r.visitors || 0}`).join('\n') : 'нет данных'}</div>
        </div>
      </div>
      <div class="card" style="margin-top:10px">
        <div class="metric-label">Мобильные ОС (90 дней)</div>
        <div class="code">${mobileOsRows.length ? mobileOsRows.map(r => `${r.os_label || r.os_root || 'Не определено'}: ${r.visits || 0} визитов`).join('\n') : 'нет данных'}</div>
      </div>
    </div>

    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Посетители в скоринге</div>
        <div class="row">
          <button class="btn" onclick="loadScoringDataAndRender()">Обновить данные</button>
          <button class="btn primary" onclick="rebuildScoring()">Пересчитать скоринг</button>
          <button class="btn ghost" onclick="buildScoringHypotheses()">Сгенерировать гипотезы</button>
        </div>
      </div>

      <div class="row scoring-filter-row mt-3">
        <div class="col-md-2 col-sm-6">
          <select id="scoring-limit-filter" class="form-select">
            <option value="50">50</option>
            <option value="100">100</option>
            <option value="200">200</option>
          </select>
        </div>
        <div class="col-md-3 col-sm-6">
          <select id="scoring-segment-filter" class="form-select">
            <option value="all">Все сегменты</option>
            <option value="hot">Hot</option>
            <option value="warm">Warm</option>
            <option value="cold">Cold</option>
          </select>
        </div>
        <div class="col-md-4 col-sm-12">
          <input id="scoring-source-filter" class="form-control" placeholder="Фильтр по source/каналу"/>
        </div>
        <div class="col-md-3 col-sm-12 d-flex gap-2">
          <button class="btn" onclick="applyScoringFilters()">Применить</button>
          <button class="btn ghost" onclick="resetScoringFilters()">Сброс</button>
        </div>
      </div>

      <div class="small muted" style="margin-top:10px">
        Всего записей: ${esc(s.total_visitors_scored ?? 0)} · В таблице: ${esc(meta.count ?? 0)} · Последний расчёт: ${esc(s.latest_scored_at || '-')}
      </div>
      ${meta.ready === false ? `<div class="code" style="margin-top:10px">Таблицы скоринга не готовы: ${esc(meta.error || 'выполните sql/040_scoring_v1.sql')}</div>` : ''}
    </div>

    <div class="panel">
      <div id="scoring-table"></div>
    </div>

    <div class="panel" id="scoring-hypothesis-panel" style="display:none">
      <div class="panel-title">Гипотезы по сегментам</div>
      <div id="scoring-hypothesis-content" class="code"></div>
    </div>
  `;

  const limitSelect = document.getElementById('scoring-limit-filter');
  const segmentSelect = document.getElementById('scoring-segment-filter');
  const sourceInput = document.getElementById('scoring-source-filter');
  if (limitSelect) limitSelect.value = String(SCORING_FILTERS.limit || 100);
  if (segmentSelect) segmentSelect.value = SCORING_FILTERS.segment || 'all';
  if (sourceInput) sourceInput.value = SCORING_FILTERS.source || '';

  initScoringCharts();
  initScoringTable(rows);
}

function renderScoringCreatives(){
  const plan = DASH.scoring_creative_plan || {};
  const cohorts = DASH.scoring_audiences_cohorts || {};
  const activationPlan = DASH.scoring_activation_plan || {};
  const reaction = DASH.scoring_activation_reaction || {};
  const items = plan.items || [];
  const activationRows = activationPlan.cohorts || [];
  const reactionRows = reaction.items || [];
  document.getElementById('section-scoring_creatives').innerHTML = `
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">План креативов по сегментам</div>
        <div class="small muted">${esc(plan.count || 0)} идей · окно ${esc(plan.days || 90)} дней</div>
      </div>
      <div class="small muted" style="margin-top:6px">Можно использовать как основу для объявлений в Direct и сегментных ретаргет-кампаний.</div>
    </div>
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Аудитории и активация в Direct</div>
        <div class="small muted">окно ${esc(activationPlan.days || cohorts.days || 90)} дней</div>
      </div>
      <div class="row" style="margin-top:8px">
        <button class="btn ghost" onclick="bootstrapScoringDirect(false)">Bootstrap Direct (dry-run)</button>
        <button class="btn ghost" onclick="bootstrapScoringDirect(true)">Bootstrap Direct (create)</button>
        <button class="btn" onclick="syncScoringDirect(true)">Dry-run sync в Direct</button>
        <button class="btn primary" onclick="syncScoringDirect(false)">Выполнить sync в Direct</button>
      </div>
      <div class="small muted" style="margin-top:10px">
        Cohorts: ${esc(activationPlan.count || 0)} · Готовы к активации: ${esc(activationPlan.eligible_count || 0)} · Минимум размера: ${esc(activationPlan.min_audience_size || 100)}
      </div>
      <div class="table-wrap">
        <table class="table">
          <thead>
            <tr>
              <th>Cohort</th>
              <th>Сегмент</th>
              <th>ОС</th>
              <th>Окно</th>
              <th>Клиенты</th>
              <th>Статус</th>
              <th>Тег</th>
            </tr>
          </thead>
          <tbody>
            ${activationRows.length ? activationRows.map(c => `
              <tr>
                <td>${esc(c.cohort_name)}</td>
                <td>${segmentBadge(c.segment)}</td>
                <td>${esc(c.os_root || 'all')}</td>
                <td>${esc(c.window_days || 0)}д</td>
                <td>${esc(c.audience_size || c.visitors || 0)}</td>
                <td>${c.eligible ? '<span class="badge good">Готов</span>' : '<span class="badge warn">Мало данных</span>'}</td>
                <td><span class="badge">${esc(c.direct_tag || '-')}</span></td>
              </tr>
            `).join('') : `<tr><td colspan="7"><div class="empty">Нет cohort-данных</div></td></tr>`}
          </tbody>
        </table>
      </div>
      <div class="code" style="margin-top:10px">Пример выгрузки: /api/scoring/audiences/export?days=90&segment=warm&os_root=android&limit=5000</div>
    </div>
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Реакция в Direct по scoring-тегам</div>
        <div class="small muted">${esc(reaction.count || 0)} тегов · ${esc(reaction.days || 30)} дней</div>
      </div>
      <div class="small muted" style="margin-top:6px">
        Impressions: ${esc((reaction.totals || {}).impressions || 0)} · Clicks: ${esc((reaction.totals || {}).clicks || 0)} · Cost: ${esc((reaction.totals || {}).cost || 0)}
      </div>
      <div class="table-wrap" style="margin-top:8px">
        <table class="table">
          <thead>
            <tr>
              <th>Тег</th>
              <th>Показы</th>
              <th>Клики</th>
              <th>CTR %</th>
              <th>CPC</th>
              <th>Расход</th>
            </tr>
          </thead>
          <tbody>
            ${reactionRows.length ? reactionRows.map(r => `
              <tr>
                <td><span class="badge">${esc(r.direct_tag || '-')}</span></td>
                <td>${esc(r.impressions || 0)}</td>
                <td>${esc(r.clicks || 0)}</td>
                <td>${esc(r.ctr_pct || 0)}</td>
                <td>${esc(r.avg_cpc || 0)}</td>
                <td>${esc(r.cost || 0)}</td>
              </tr>
            `).join('') : `<tr><td colspan="6"><div class="empty">Нет данных реакции. Создайте кампании с тегом scoring_* в названии.</div></td></tr>`}
          </tbody>
        </table>
      </div>
    </div>
    <div class="table-wrap">
      <table class="table">
        <thead>
          <tr>
            <th>Сегмент</th>
            <th>Причина</th>
            <th>Угол</th>
            <th>Заголовок</th>
            <th>Текст</th>
            <th>CTA</th>
            <th>Гипотеза</th>
            <th>Тег</th>
          </tr>
        </thead>
        <tbody>
          ${items.length ? items.map(r => `
            <tr>
              <td>${segmentBadge(r.segment)}</td>
              <td>${esc(r.short_reason || '-')}</td>
              <td>${esc(r.creative_angle || '-')}</td>
              <td>${esc(r.headline || '-')}</td>
              <td>${esc(shortText(r.body || '-', 120))}</td>
              <td>${esc(r.cta || '-')}</td>
              <td>${esc(shortText(r.hypothesis || '-', 120))}</td>
              <td><span class="badge">${esc(r.direct_tag || '-')}</span></td>
            </tr>
          `).join('') : `<tr><td colspan="8"><div class="empty">Нет данных для плана креативов</div></td></tr>`}
        </tbody>
      </table>
    </div>
  `;
}

function renderScoringTemplates(){
  const payload = DASH.scoring_ad_templates || {};
  const items = payload.items || [];
  const days = Number(payload.days || 90);
  const variants = Number(payload.variants || 3);
  const minAudience = Number(payload.min_audience_size || 1);
  const includeSmall = Boolean(payload.include_small);
  const isReady = payload.ready !== false;

  const totalVariants = items.reduce((acc, row) => acc + ((row.variants || []).length), 0);

  document.getElementById('section-scoring_templates').innerHTML = `
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Шаблоны объявлений по cohort</div>
        <div class="small muted">cohorts: ${esc(payload.count || 0)} · вариантов: ${esc(totalVariants)} · окно: ${esc(days)}д</div>
      </div>
      <div class="small muted" style="margin-top:6px">Для каждой группы показаны несколько текстовых вариаций и объяснение, почему выбран такой угол.</div>
      <div class="row scoring-filter-row mt-3">
        <div class="col-md-2 col-sm-6">
          <select id="templates-days-filter" class="form-select">
            <option value="30" ${days === 30 ? 'selected' : ''}>30 дней</option>
            <option value="60" ${days === 60 ? 'selected' : ''}>60 дней</option>
            <option value="90" ${days === 90 ? 'selected' : ''}>90 дней</option>
          </select>
        </div>
        <div class="col-md-2 col-sm-6">
          <select id="templates-min-filter" class="form-select">
            <option value="1" ${minAudience === 1 ? 'selected' : ''}>min 1</option>
            <option value="50" ${minAudience === 50 ? 'selected' : ''}>min 50</option>
            <option value="100" ${minAudience === 100 ? 'selected' : ''}>min 100</option>
          </select>
        </div>
        <div class="col-md-2 col-sm-6">
          <select id="templates-variants-filter" class="form-select">
            <option value="1" ${variants === 1 ? 'selected' : ''}>1 вариант</option>
            <option value="2" ${variants === 2 ? 'selected' : ''}>2 варианта</option>
            <option value="3" ${variants === 3 ? 'selected' : ''}>3 варианта</option>
            <option value="4" ${variants === 4 ? 'selected' : ''}>4 варианта</option>
            <option value="5" ${variants === 5 ? 'selected' : ''}>5 вариантов</option>
          </select>
        </div>
        <div class="col-md-3 col-sm-6 d-flex align-items-center">
          <div class="form-check">
            <input class="form-check-input" type="checkbox" id="templates-include-small" ${includeSmall ? 'checked' : ''}>
            <label class="form-check-label" for="templates-include-small">Включать малые аудитории</label>
          </div>
        </div>
        <div class="col-md-3 col-sm-12 d-flex gap-2">
          <button class="btn" onclick="applyScoringTemplateFilters()">Применить</button>
          <button class="btn ghost" onclick="loadScoringDataAndRender()">Обновить</button>
        </div>
      </div>
      ${isReady ? '' : `<div class="code" style="margin-top:10px">API ad-templates недоступен: ${esc(payload.error || 'unknown error')}</div>`}
    </div>

    ${items.length ? items.map((row) => `
      <div class="panel">
        <div class="row" style="justify-content:space-between">
          <div>
            <div><b>${esc(row.cohort_name || '-')}</b> · ${segmentBadge(row.segment)}</div>
            <div class="small muted">
              Аудитория: ${esc(row.audience_size || 0)} · Окно: ${esc(row.window_days || days)}д · ОС: ${esc((row.os_root || 'all').toUpperCase())}
            </div>
          </div>
          <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;justify-content:flex-end">
            <div class="small muted">Тег: <span class="badge">${esc(row.direct_tag || '-')}</span></div>
            <button class="btn primary" onclick="generateScoringBanners(${JSON.stringify(row.cohort_name || '')})">
              Сгенерировать баннеры
            </button>
          </div>
        </div>

        <div class="grid-2" style="margin-top:10px">
          <div class="card">
            <div class="metric-label">Группа Direct</div>
            <div class="code">ad_group_id: ${esc(row.ad_group_id || '-')}
retargeting_list_id: ${esc(row.retargeting_list_id || '-')}
goal_id: ${esc(row.goal_id || '-')}
priority: ${esc(row.strategy_priority || '-')}</div>
          </div>
          <div class="card">
            <div class="metric-label">Подтверждение группы</div>
            <div class="code">segment: ${esc(row.segment || '-')}
short_reason_hint: ${esc(reasonLabel(row.short_reason_hint || 'unknown'))}
source_hint: ${esc(sourceLabel(row.source_hint || 'unknown'))}</div>
          </div>
        </div>

        <div class="card" style="margin-top:10px">
          <div class="metric-label">KPI-гипотеза (CTR/STR и конверсия)</div>
          <div class="code">Цель: ${esc(row.kpi_hypothesis?.objective || '-')}
Окно сравнения: ${esc(row.kpi_hypothesis?.comparison_window_days || '-')}д

Экономика:
- Средний чек: ${esc(numText(row.kpi_hypothesis?.economics?.avg_check_rub, 0))} ₽
- Маржа: ${esc(numText(row.kpi_hypothesis?.economics?.margin_pct, 0))}% (${esc(numText(row.kpi_hypothesis?.economics?.margin_rub, 0))} ₽)
- Доля CAC от маржи (потолок): ${esc(numText(row.kpi_hypothesis?.economics?.max_marketing_share_of_margin_pct, 1))}%

Baseline (если уже есть реакция):
- Показы: ${esc(row.kpi_hypothesis?.baseline?.impressions ?? '-')}
- Клики: ${esc(row.kpi_hypothesis?.baseline?.clicks ?? '-')}
- CTR(STR): ${esc(numText(row.kpi_hypothesis?.baseline?.ctr_pct, 2))}%
- CPC: ${esc(numText(row.kpi_hypothesis?.baseline?.avg_cpc_rub, 2))} ₽
- Расход: ${esc(numText(row.kpi_hypothesis?.baseline?.cost_rub, 2))} ₽

Что ожидаем:
- CTR(STR): >= ${esc(numText(row.kpi_hypothesis?.expected?.ctr_str_min_pct, 2))}% (target ${esc(numText(row.kpi_hypothesis?.expected?.ctr_str_target_pct, 2))}%)
- CR клик->заявка: >= ${esc(numText(row.kpi_hypothesis?.expected?.click_to_lead_min_pct ?? row.kpi_hypothesis?.expected?.cvr_to_lead_min_pct, 2))}% (target ${esc(numText(row.kpi_hypothesis?.expected?.click_to_lead_target_pct ?? row.kpi_hypothesis?.expected?.cvr_to_lead_target_pct, 2))}%)
- Факт CR клик->заявка (${esc(row.kpi_hypothesis?.expected?.reference_window_days ?? row.conversion_reference?.selected_window_days ?? '-') }д): ${esc(numText(row.kpi_hypothesis?.expected?.click_to_lead_actual_pct, 2))}% (${esc(row.kpi_hypothesis?.expected?.click_to_lead_basis || 'model')})
- Цена клика (CPC): target <= ${esc(numText(row.kpi_hypothesis?.expected?.target_cpc_rub, 2))} ₽ (max ${esc(numText(row.kpi_hypothesis?.expected?.max_cpc_rub ?? row.kpi_hypothesis?.expected?.avg_cpc_max_rub, 2))} ₽)
- Цена заявки (CPL): target <= ${esc(numText(row.kpi_hypothesis?.expected?.target_cpl_rub, 2))} ₽ (max ${esc(numText(row.kpi_hypothesis?.expected?.max_cpl_rub, 2))} ₽)
- Цена оплаты (CAC): target <= ${esc(numText(row.kpi_hypothesis?.expected?.target_cac_pay_rub ?? row.kpi_hypothesis?.expected?.target_cpa_rub, 2))} ₽ (max ${esc(numText(row.kpi_hypothesis?.expected?.max_cac_pay_rub, 2))} ₽)
- CR заявка->оплата (модель): ${esc(numText(row.kpi_hypothesis?.expected?.lead_to_payment_cvr_pct, 2))}%

Минимум для оценки:
- Показы: ${esc(row.kpi_hypothesis?.sample_gate?.min_impressions ?? '-')}
- Клики: ${esc(row.kpi_hypothesis?.sample_gate?.min_clicks ?? '-')}

Что важно отслеживать:
- Основные: ${esc(((row.kpi_hypothesis?.primary_metrics || []).map(m => m.label).join(', ')) || '-')}
- Дополнительно: ${esc(((row.kpi_hypothesis?.secondary_metrics || []).map(m => m.label).join(', ')) || '-')}

Критерий успеха:
${esc(row.kpi_hypothesis?.success_rule || '-')}

Примечание:
${esc(row.kpi_hypothesis?.data_gap_note || '-')}</div>
        </div>

        <div class="table-wrap" style="margin-top:10px">
          <table class="table">
            <thead>
              <tr>
                <th>Вариант</th>
                <th>Угол</th>
                <th>Заголовок</th>
                <th>Текст</th>
                <th>CTA</th>
                <th>Почему так</th>
              </tr>
            </thead>
            <tbody>
              ${(row.variants || []).length ? (row.variants || []).map((v, idx) => `
                <tr>
                  <td><span class="badge">${esc(v.variant_key || `v${idx + 1}`)}</span></td>
                  <td>${esc(v.creative_angle || '-')}</td>
                  <td>${esc(v.headline || '-')}</td>
                  <td>${esc(shortText(v.body || '-', 180))}</td>
                  <td>${esc(v.cta || '-')}</td>
                  <td>${esc(shortText(v.why_this || '-', 200))}</td>
                </tr>
              `).join('') : `<tr><td colspan="6"><div class="empty">Нет вариантов для cohort</div></td></tr>`}
            </tbody>
          </table>
        </div>

        <div class="card" style="margin-top:10px">
          <div class="metric-label">Сгенерированные баннеры</div>
          <div id="banners-${safeDomId(row.cohort_name || '')}" class="small muted" style="margin-top:8px">
            Пока не сгенерировано. Нажмите «Сгенерировать баннеры».
          </div>
        </div>
      </div>
    `).join('') : `
      <div class="panel">
        <div class="empty">Нет cohort-данных для шаблонов. Проверьте scoring и activation plan.</div>
      </div>
    `}
  `;
}

async function loadScoringData(){
  const params = new URLSearchParams();
  params.set('limit', String(SCORING_FILTERS.limit || 100));
  if (SCORING_FILTERS.segment && SCORING_FILTERS.segment !== 'all') {
    params.set('segment', SCORING_FILTERS.segment);
  }
  if ((SCORING_FILTERS.source || '').trim()) {
    params.set('source', SCORING_FILTERS.source.trim());
  }
  const templatesState = DASH.scoring_ad_templates || {};
  const templatesParams = new URLSearchParams();
  templatesParams.set('days', String(Math.max(1, Math.min(Number(templatesState.days || 90), 365))));
  templatesParams.set('min_audience_size', String(Math.max(1, Math.min(Number(templatesState.min_audience_size || 1), 100000))));
  templatesParams.set('variants', String(Math.max(1, Math.min(Number(templatesState.variants || 3), 5))));
  templatesParams.set('include_small', String(Boolean(templatesState.include_small !== false)));

  const [summaryRes, visitorsRes, timeseriesRes, audienceRes, attributionRes, creativePlanRes, cohortsRes, activationPlanRes, reactionRes, adTemplatesRes] = await Promise.allSettled([
    api('/api/scoring/summary'),
    api('/api/scoring/visitors?' + params.toString()),
    api('/api/scoring/timeseries?days=90'),
    api('/api/scoring/audience?days=90'),
    api('/api/scoring/attribution-quality?days=90'),
    api('/api/scoring/creative-plan?days=90&limit_per_segment=5'),
    api('/api/scoring/audiences/cohorts?days=90'),
    api('/api/scoring/activation/plan?days=90&min_audience_size=100&export_limit=5000'),
    api('/api/scoring/activation/reaction?days=30&limit=50'),
    api('/api/scoring/ad-templates?' + templatesParams.toString()),
  ]);

  const summary = summaryRes.status === 'fulfilled'
    ? summaryRes.value
    : { ready: false, error: summaryRes.reason?.message || 'summary api failed' };
  const visitors = visitorsRes.status === 'fulfilled'
    ? visitorsRes.value
    : { ready: false, items: [], count: 0, limit: SCORING_FILTERS.limit || 100, error: visitorsRes.reason?.message || 'visitors api failed' };
  const timeseries = timeseriesRes.status === 'fulfilled'
    ? timeseriesRes.value
    : { ready: false, dates: [], hot: [], warm: [], cold: [], error: timeseriesRes.reason?.message || 'timeseries api failed' };
  const audience = audienceRes.status === 'fulfilled'
    ? audienceRes.value
    : { ready: false, gender_age: [], source_mix: [], device_mix: [], error: audienceRes.reason?.message || 'audience api failed' };
  const attribution = attributionRes.status === 'fulfilled'
    ? attributionRes.value
    : { ready: false, status: 'error', direct_pct: 0, unknown_pct: 0, top_sources: [], error: attributionRes.reason?.message || 'attribution api failed' };
  const creativePlan = creativePlanRes.status === 'fulfilled'
    ? creativePlanRes.value
    : { ready: false, items: [], count: 0, days: 90, error: creativePlanRes.reason?.message || 'creative plan api failed' };
  const cohorts = cohortsRes.status === 'fulfilled'
    ? cohortsRes.value
    : { ready: false, cohorts: [], matrix: [], days: 90, error: cohortsRes.reason?.message || 'cohorts api failed' };
  const activationPlan = activationPlanRes.status === 'fulfilled'
    ? activationPlanRes.value
    : { ready: false, cohorts: [], count: 0, eligible_count: 0, min_audience_size: 100, error: activationPlanRes.reason?.message || 'activation plan api failed' };
  const reaction = reactionRes.status === 'fulfilled'
    ? reactionRes.value
    : { ready: false, items: [], count: 0, totals: { impressions: 0, clicks: 0, cost: 0 }, error: reactionRes.reason?.message || 'activation reaction api failed' };
  const adTemplates = adTemplatesRes.status === 'fulfilled'
    ? adTemplatesRes.value
    : {
      ready: false,
      days: Number(templatesState.days || 90),
      min_audience_size: Number(templatesState.min_audience_size || 1),
      include_small: Boolean(templatesState.include_small !== false),
      variants: Number(templatesState.variants || 3),
      count: 0,
      items: [],
      error: adTemplatesRes.reason?.message || 'ad templates api failed',
    };

  DASH.scoring_summary = summary || {};
  DASH.scoring_visitors = visitors.items || [];
  DASH.scoring_timeseries = normalizeTimeseries(timeseries || {});
  DASH.scoring_audience = audience || { ready: false, gender_age: [], source_mix: [], device_mix: [] };
  DASH.scoring_attribution = attribution || { ready: false, status: 'empty', direct_pct: 0, unknown_pct: 0, top_sources: [] };
  DASH.scoring_creative_plan = creativePlan || { ready: false, items: [], count: 0, days: 90 };
  DASH.scoring_audiences_cohorts = cohorts || { ready: false, cohorts: [], matrix: [], days: 90 };
  DASH.scoring_activation_plan = activationPlan || { ready: false, cohorts: [], count: 0, eligible_count: 0, min_audience_size: 100 };
  DASH.scoring_activation_reaction = reaction || { ready: false, items: [], count: 0, totals: { impressions: 0, clicks: 0, cost: 0 } };
  DASH.scoring_ad_templates = adTemplates || { ready: false, items: [], count: 0, days: 90, min_audience_size: 1, include_small: true, variants: 3 };
  DASH.scoring_meta = {
    ready: (summary.ready !== false) && (visitors.ready !== false) && (timeseries.ready !== false),
    count: visitors.count ?? (DASH.scoring_visitors || []).length,
    limit: visitors.limit ?? 100,
    error: visitors.error || summary.error || timeseries.error || '',
  };
}

async function loadScoringDataAndRender(){
  try{
    await loadScoringData();
    if (CURRENT_SECTION === 'scoring') renderScoring();
    if (CURRENT_SECTION === 'scoring_creatives') renderScoringCreatives();
    if (CURRENT_SECTION === 'scoring_templates') renderScoringTemplates();
  }catch(e){
    alert('Ошибка загрузки скоринга: ' + e.message);
  }
}

async function applyScoringFilters(){
  SCORING_FILTERS.limit = normalizeScoringLimit(document.getElementById('scoring-limit-filter')?.value || 100);
  SCORING_FILTERS.segment = document.getElementById('scoring-segment-filter')?.value || 'all';
  SCORING_FILTERS.source = document.getElementById('scoring-source-filter')?.value || '';
  await loadScoringDataAndRender();
}

async function resetScoringFilters(){
  SCORING_FILTERS = { segment: 'all', source: '', limit: 100 };
  await loadScoringDataAndRender();
}

async function applyScoringTemplateFilters(){
  const days = Number(document.getElementById('templates-days-filter')?.value || 90);
  const minAudience = Number(document.getElementById('templates-min-filter')?.value || 1);
  const variants = Number(document.getElementById('templates-variants-filter')?.value || 3);
  const includeSmall = Boolean(document.getElementById('templates-include-small')?.checked);

  DASH.scoring_ad_templates = {
    ...(DASH.scoring_ad_templates || {}),
    days: Math.max(1, Math.min(days, 365)),
    min_audience_size: Math.max(1, Math.min(minAudience, 100000)),
    variants: Math.max(1, Math.min(variants, 5)),
    include_small: includeSmall,
  };
  await loadScoringDataAndRender();
}

async function generateScoringBanners(cohortName){
  const name = String(cohortName || '').trim();
  if (!name) return;

  const containerId = 'banners-' + safeDomId(name);
  const container = document.getElementById(containerId);
  if (container) {
    container.innerHTML = '<div class="small muted">Генерация баннеров...</div>';
  }

  const tpl = DASH.scoring_ad_templates || {};
  try {
    const data = await api('/api/scoring/ad-templates/generate-banners', {
      method: 'POST',
      body: JSON.stringify({
        cohort_name: name,
        days: Number(tpl.days || 90),
        min_audience_size: Number(tpl.min_audience_size || 1),
        include_small: Boolean(tpl.include_small !== false),
        variants: Number(tpl.variants || 3),
        images_per_variant: 1,
        size: '1536x1024',
        quality: 'medium',
        output_format: 'png',
      }),
    });

    const images = data.generated || [];
    if (!container) return;
    if (!images.length) {
      container.innerHTML = '<div class="small muted">Генерация завершена, но изображения не получены.</div>';
      return;
    }

    container.innerHTML = `
      <div class="small muted" style="margin-bottom:8px">
        Сгенерировано: ${esc(data.generated_count || images.length)} · Модель: ${esc(data.model || '-')} · Размер: ${esc(data.size || '-')}
      </div>
      <div class="banner-grid">
        ${images.map((img) => `
          <div class="banner-card">
            <a href="${esc(img.static_url)}" target="_blank" rel="noopener noreferrer">
              <img class="banner-image" src="${esc(img.static_url)}" alt="${esc(img.variant_key || 'banner')}"/>
            </a>
            <div class="small" style="margin-top:8px"><b>${esc(img.variant_key || '-')}</b></div>
            <div class="small muted">${esc(shortText(img.headline || '', 80))}</div>
            <div class="small muted">${esc(img.cta || '')}</div>
          </div>
        `).join('')}
      </div>
      ${(data.failed_count || 0) > 0 ? `<div class="code" style="margin-top:10px">Ошибок генерации: ${esc(data.failed_count)}</div>` : ''}
    `;
  } catch (e) {
    if (container) {
      container.innerHTML = `<div class="code">Ошибка генерации баннеров: ${esc(e.message)}</div>`;
    } else {
      alert('Ошибка генерации баннеров: ' + e.message);
    }
  }
}

async function rebuildScoring(){
  const data = await api('/api/scoring/rebuild', {
    method:'POST',
    body: JSON.stringify({
      use_fallback: true,
      sync_features: true,
      features_days: 90
    })
  });
  alert(`Скоринг пересчитан. Обработано: ${data.processed || 0}, записано: ${data.upserted || 0}, источник: ${data.source_mode || '-'}`);
  await loadScoringDataAndRender();
}

async function syncScoringDirect(dryRun=true){
  if (!dryRun) {
    const ok = confirm('Выполнить запись в Direct API? Проверьте SCORING_DIRECT_SYNC_ENABLED и mapping.');
    if (!ok) return;
  }
  try {
    const data = await api('/api/scoring/activation/direct-sync', {
      method: 'POST',
      body: JSON.stringify({
        days: 90,
        min_audience_size: 100,
        export_limit: 5000,
        dry_run: !!dryRun
      })
    });
    const sync = data.sync || {};
    alert(
      `Direct sync: ${data.dry_run ? 'dry-run' : 'execute'}\n` +
      `eligible: ${data.eligible_count || 0}\n` +
      `attempted: ${sync.attempted || 0}\n` +
      `applied: ${sync.applied || 0}\n` +
      `skipped: ${sync.skipped || 0}\n` +
      `errors: ${sync.errors || 0}`
    );
    await loadScoringDataAndRender();
  } catch (e) {
    alert('Ошибка sync в Direct: ' + e.message);
  }
}

async function bootstrapScoringDirect(apply=false){
  if (apply) {
    const ok = confirm('Создать сущности в Direct и записать ID в env?');
    if (!ok) return;
  }
  try {
    const data = await api('/api/scoring/activation/bootstrap-direct', {
      method: 'POST',
      body: JSON.stringify({
        days: 90,
        min_audience_size: 100,
        export_limit: 5000,
        apply: !!apply,
        include_small: false,
      })
    });
    const b = data.bootstrap || {};
    alert(
      `Bootstrap Direct: ${data.apply ? 'create' : 'dry-run'}\n` +
      `campaign_id: ${data.campaign_id || '-'}\n` +
      `cohorts_selected: ${data.cohorts_selected || 0}\n` +
      `created_adgroups: ${b.created_adgroups || 0}\n` +
      `created_lists: ${b.created_retargeting_lists || 0}\n` +
      `attached_targets: ${b.attached_audience_targets || 0}\n` +
      `errors: ${b.errors || 0}`
    );
    await loadScoringDataAndRender();
  } catch (e) {
    alert('Ошибка bootstrap Direct: ' + e.message);
  }
}

async function openScoringDetails(visitorId){
  if (!visitorId) return;
  let data = {};
  try {
    data = await api('/api/scoring/visitor/' + encodeURIComponent(visitorId));
  } catch (e) {
    alert('Не удалось загрузить visitor detail: ' + e.message);
    return;
  }
  const explanation = (data && typeof data.explanation_json === 'object' && data.explanation_json) ? data.explanation_json : {};

  const sourceText = [sourceLabel(data.traffic_source), data.utm_source, data.utm_medium]
    .filter(Boolean)
    .join(' / ');
  const sourceMode = data.source_mode || data.data_source || '-';

  const factorRows = Object.entries(explanation)
    .sort((a, b) => Number(b[1] || 0) - Number(a[1] || 0))
    .map(([k, v]) => {
      const num = Number(v || 0);
      const signed = num > 0 ? `+${num}` : String(num);
      return `- ${factorLabel(k)}: ${signed}`;
    })
    .join('\n');

  const scoreText = Number((data.score ?? data.normalized_score ?? 0)).toFixed(2);
  const segmentRaw = String(data.segment || '').toLowerCase();
  const segmentText = segmentRaw === 'hot' ? 'ГОРЯЧИЙ' : segmentRaw === 'warm' ? 'ТЁПЛЫЙ' : segmentRaw === 'cold' ? 'ХОЛОДНЫЙ' : '-';
  const reasonText = data.short_reason || '-';
  const humanText = data.human_explanation || '-';
  const actionText = data.recommended_action || data.recommendation || '-';

  const html = `
    <div class="panel">
      <div class="panel-title">Visitor ${esc(data.visitor_id || '')}</div>
    </div>

    <div class="panel">
      <div class="panel-title">Блок 1: Итог</div>
      <div class="code">- Скор: ${esc(scoreText)}
- Сегмент: ${esc(segmentText)}
- Причина: ${esc(reasonText)}</div>
    </div>

    <div class="panel">
      <div class="panel-title">Блок 2: Почему такой скор</div>
      <div class="code">${esc(humanText)}</div>
    </div>

    <div class="panel">
      <div class="panel-title">Блок 3: Факторы</div>
      <div class="code">${esc(factorRows || '- нет сработавших факторов')}</div>
    </div>

    <div class="panel">
      <div class="panel-title">Блок 4: Что делать</div>
      <div class="code">${esc(actionText)}</div>
    </div>

    <div class="panel">
      <div class="panel-title">Блок 5: Техническое</div>
      <div class="code">- source: ${esc(sourceText || '-')}
- source_mode: ${esc(sourceMode)}
- рассчитан: ${esc(data.scored_at || '-')}</div>
    </div>
  `;

  document.getElementById('scoring-drawer-content').innerHTML = html;
  document.getElementById('scoring-drawer').style.display = 'block';
}

function closeScoringDetails(){
  const drawer = document.getElementById('scoring-drawer');
  if (drawer) drawer.style.display = 'none';
}

function buildScoringHypotheses(){
  const s = DASH.scoring_summary || {};
  const rows = DASH.scoring_visitors || [];
  const audience = DASH.scoring_audience || {};
  const hotPct = Number(s.total_visitors_scored || 0) > 0 ? ((Number(s.hot_count || 0) / Number(s.total_visitors_scored || 1)) * 100) : 0;
  const warmPct = Number(s.total_visitors_scored || 0) > 0 ? ((Number(s.warm_count || 0) / Number(s.total_visitors_scored || 1)) * 100) : 0;
  const coldPct = Number(s.total_visitors_scored || 0) > 0 ? ((Number(s.cold_count || 0) / Number(s.total_visitors_scored || 1)) * 100) : 0;

  const topSource = (audience.source_mix || [])[0];
  const topGenderAge = (audience.gender_age || [])[0];
  const topReason = (rows.map(r => r.short_reason).filter(Boolean).reduce((acc, x) => (acc[x] = (acc[x] || 0) + 1, acc), {}));
  const reasonSorted = Object.entries(topReason).sort((a,b) => b[1]-a[1]);

  const lines = [
    `1) Сегментный баланс: горячие ${hotPct.toFixed(1)}%, тёплые ${warmPct.toFixed(1)}%, холодные ${coldPct.toFixed(1)}%.`,
    hotPct < 15
      ? '2) Гипотеза: мало горячих лидов. Тест: усилить CTA и ретаргет на тёплых (7 дней, цель — рост hot доли).'
      : '2) Гипотеза: поток горячих стабильный. Тест: ускорить обработку hot-лидов (время ответа < 10 минут).',
    reasonSorted.length
      ? `3) Основная причина сегментации: ${reasonSorted[0][0]} (${reasonSorted[0][1]} клиентов в текущем срезе).`
      : '3) Недостаточно причин в срезе для ранжирования.',
    topSource
      ? `4) Главный канал: ${sourceLabel(topSource.source)} (${topSource.visitors} клиентов).`
      : '4) Канал не определён — нужен аудит атрибуции.',
    topGenderAge
      ? `5) Аудитория: ${topGenderAge.gender}, ${topGenderAge.age_interval} (визиты: ${topGenderAge.visits}).`
      : '5) Недостаточно данных по полу/возрасту за выбранный период.',
  ];

  const panel = document.getElementById('scoring-hypothesis-panel');
  const content = document.getElementById('scoring-hypothesis-content');
  if (content) content.textContent = lines.join('\n');
  if (panel) panel.style.display = 'block';
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
  if (!IS_SCORING_STANDALONE) {
    renderSummary();
    renderCampaignFilter();
  }

  if (CURRENT_SECTION === 'overview') renderOverview();
  if (CURRENT_SECTION === 'creatives') renderCreatives();
  if (CURRENT_SECTION === 'structure') renderStructure();
  if (CURRENT_SECTION === 'negatives') renderNegatives();
  if (CURRENT_SECTION === 'forecast') renderForecast();
  if (CURRENT_SECTION === 'scoring') renderScoring();
  if (CURRENT_SECTION === 'scoring_creatives') renderScoringCreatives();
  if (CURRENT_SECTION === 'scoring_templates') renderScoringTemplates();
  if (CURRENT_SECTION === 'actions') renderActions();
  if (CURRENT_SECTION === 'diagnostics') renderDiagnostics();
}

async function reloadAll(){
  try{
    const dashboard = await api('/api/full-dashboard');
    DASH = {
      ...DASH,
      ...dashboard,
    };
    await loadScoringData();
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
  applyInitialRouteState();
  applyStandaloneScoringLayout();
  document.getElementById('global-search').addEventListener('input', renderSection);
  document.getElementById('campaign-filter').addEventListener('change', renderSection);
  document.getElementById('status-filter').addEventListener('change', renderSection);
  const drawer = document.getElementById('scoring-drawer');
  if (drawer) {
    drawer.addEventListener('click', (e) => {
      if (e.target.id === 'scoring-drawer') closeScoringDetails();
    });
  }
  if (CURRENT_SECTION !== 'overview') {
    setSection(CURRENT_SECTION);
  }
  if (IS_SCORING_STANDALONE) {
    await loadScoringData();
    renderSection();
  } else {
    await reloadAll();
  }
});
