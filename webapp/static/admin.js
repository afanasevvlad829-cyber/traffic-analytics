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
  scoring_meta: { ready: true, count: 0, limit: 100, error: '' },
  crm_load_status: { ready: false, items: [], count: 0, error: '' },
  crm_users: { ready: false, items: [], count: 0, limit: 100, offset: 0, error: '' },
  crm_communications: { ready: false, items: [], count: 0, limit: 100, offset: 0, error: '' },
  crm_meta: { ready: false, selected_customer_id: null, selected_customer_name: '' },
  audits_health: { ok: false, checked_at: null, latency_ms: null, error_class: '', error: '', retryable: false },
  audits_runs: { items: [], count: 0, error: '' },
  audits_meta: { selected_run_id: null, selected_run: null, run_result: null, loading: false }
};

let CURRENT_SECTION = 'overview';
let IS_SCORING_STANDALONE = false;
let IS_AUDITS_STANDALONE = false;
let SCORING_FILTERS = { segment: 'all', source: '', limit: 100 };
let CRM_FILTERS = { report_date: '', segment: 'all', q: '', limit: 100, offset: 0, communications_limit: 100, communications_offset: 0 };
let CRM_SELECTED_CUSTOMER_ID = null;
let CRM_SYNC_OPTIONS = { updates_only: true, include_communications: true, include_lessons: false, include_extra: false, timeout_sec: 1800 };
let SCORING_TABLE = null;
let SCORING_TIMESERIES_CHART = null;
let SCORING_DISTRIBUTION_CHART = null;
const BANNER_GENERATION_JOBS = {};

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
  if (path === '/admin/audits') {
    CURRENT_SECTION = 'audits';
    IS_AUDITS_STANDALONE = true;
  }
  if (['overview', 'creatives', 'structure', 'negatives', 'forecast', 'scoring', 'scoring_creatives', 'scoring_templates', 'crm', 'audits', 'actions', 'diagnostics'].includes(section)) {
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

  const crmDate = String(params.get('crm_report_date') || '').trim();
  if (crmDate) {
    CRM_FILTERS.report_date = crmDate;
  }
  const crmSegment = String(params.get('crm_segment') || '').trim();
  if (crmSegment) {
    CRM_FILTERS.segment = crmSegment;
  }
  const crmQ = String(params.get('crm_q') || '').trim();
  if (crmQ) {
    CRM_FILTERS.q = crmQ;
  }
  if (params.has('crm_limit')) {
    const n = Number(params.get('crm_limit'));
    if ([50, 100, 200].includes(n)) CRM_FILTERS.limit = n;
  }
}

function applyStandaloneScoringLayout(){
  if (!IS_SCORING_STANDALONE) return;
  document.body.classList.add('scoring-standalone');

  const title = document.getElementById('topbar-title');
  const subtitle = document.getElementById('topbar-subtitle');
  const actions = document.getElementById('topbar-actions');
  if (CURRENT_SECTION === 'scoring_creatives') {
    if (title) title.textContent = 'Аудитории и активация';
    if (subtitle) subtitle.textContent = 'Сегменты, выгрузки и активация в Директ для горячих, тёплых и холодных аудиторий';
  } else if (CURRENT_SECTION === 'scoring_templates') {
    if (title) title.textContent = 'Шаблоны баннеров';
    if (subtitle) subtitle.textContent = 'Варианты креативов по сегментам с гипотезой и привязкой к группам Директ';
  } else {
    if (title) title.textContent = 'Скоринг посетителей';
    if (subtitle) subtitle.textContent = 'Оценка вероятности покупки и рекомендации для маркетинга';
  }
  if (actions) {
    const sectionLinks = CURRENT_SECTION === 'scoring'
      ? `<a class="btn ghost" href="/admin/scoring/creatives">Аудитории</a>
         <a class="btn ghost" href="/admin/scoring/templates">Шаблоны</a>`
      : CURRENT_SECTION === 'scoring_creatives'
        ? `<a class="btn ghost" href="/admin/scoring">Скоринг</a>
           <a class="btn ghost" href="/admin/scoring/templates">Шаблоны</a>`
        : `<a class="btn ghost" href="/admin/scoring">Скоринг</a>
           <a class="btn ghost" href="/admin/scoring/creatives">Аудитории</a>`;

    actions.innerHTML = `
      <a class="btn ghost" href="/admin">Открыть обзор</a>
      ${sectionLinks}
      <button class="btn" onclick="loadScoringDataAndRender()">Обновить данные</button>
      <button class="btn primary" onclick="rebuildScoring()">Пересчитать скоринг</button>
    `;
  }

  document.querySelectorAll('.nav button').forEach(btn => {
    const section = btn.dataset.section || '';
    const scoringSections = ['scoring', 'scoring_creatives', 'scoring_templates'];
    const labelNode = btn.querySelector('.nav-label');

    if (!scoringSections.includes(section)) {
      btn.setAttribute('onclick', `window.location.href='/admin?section=${encodeURIComponent(section)}'`);
    } else {
      const label = section === 'scoring'
        ? 'Скоринг посетителей'
        : section === 'scoring_creatives'
          ? 'Аудитории и активация'
          : 'Шаблоны баннеров';
      if (labelNode) {
        labelNode.textContent = label;
      } else {
        btn.textContent = label;
      }
    }
  });
}

function applyStandaloneAuditsLayout(){
  if (!IS_AUDITS_STANDALONE) return;
  document.body.classList.add('scoring-standalone');

  const title = document.getElementById('topbar-title');
  const subtitle = document.getElementById('topbar-subtitle');
  const actions = document.getElementById('topbar-actions');

  if (title) title.textContent = 'Аудит OpenRouter';
  if (subtitle) subtitle.textContent = 'Проверка health канала, запусков аудита и ошибок исполнения';

  if (actions) {
    actions.innerHTML = `
      <a class="btn ghost" href="/admin">Открыть обзор</a>
      <button class="btn" onclick="loadAuditsDataAndRender()">Обновить данные</button>
      <button class="btn primary" onclick="runAuditSmokeRun()">Тестовый запуск аудита</button>
    `;
  }

  document.querySelectorAll('.nav button').forEach(btn => {
    const section = btn.dataset.section || '';
    if (section !== 'audits') {
      btn.setAttribute('onclick', `window.location.href='/admin?section=${encodeURIComponent(section)}'`);
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

function safeMetaLink(url){
  const v = String(url || '').trim();
  if (!v || !/^https?:\/\//i.test(v)) return '';
  return v;
}

async function loadSystemVersionMeta(){
  try {
    const data = await api('/api/system/version');
    const branchRow = document.getElementById('meta-branch-row');
    const branchValue = document.getElementById('meta-branch-value');
    const commitRow = document.getElementById('meta-commit-row');
    const commitValue = document.getElementById('meta-commit-value');
    const latestRow = document.getElementById('meta-latest-row');
    const latestLink = document.getElementById('meta-latest-link');

    if (branchValue) {
      const branchText = data.branch || '-';
      const branchUrl = safeMetaLink(data.branch_url);
      if (branchUrl) {
        branchValue.textContent = '';
        const a = document.createElement('a');
        a.className = 'sidebar-meta-link';
        a.href = branchUrl;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.textContent = branchText;
        branchValue.appendChild(a);
      } else {
        branchValue.textContent = branchText;
      }
    }

    if (commitValue) {
      const commitText = data.commit_short || '-';
      const commitUrl = safeMetaLink(data.commit_url);
      if (commitUrl) {
        commitValue.textContent = '';
        const a = document.createElement('a');
        a.className = 'sidebar-meta-link';
        a.href = commitUrl;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.textContent = commitText;
        commitValue.appendChild(a);
      } else {
        commitValue.textContent = commitText;
      }
    }

    if (latestLink && latestRow) {
      const latestUrl = safeMetaLink(data.latest_changes_url || data.repo_url);
      if (latestUrl) {
        latestLink.href = latestUrl;
        latestLink.textContent = 'GitHub';
      } else {
        latestRow.style.display = 'none';
      }
    }

    if (branchRow) branchRow.style.display = '';
    if (commitRow) commitRow.style.display = '';
  } catch (e) {
    const latestRow = document.getElementById('meta-latest-row');
    if (latestRow) latestRow.style.display = 'none';
  }
}

async function api(path, options={}){
  const timeoutMs = Number(options.timeoutMs || 0);
  const controller = new AbortController();
  const fetchOptions = { ...options };
  delete fetchOptions.timeoutMs;

  let timeoutId = null;
  if (timeoutMs > 0) {
    timeoutId = window.setTimeout(() => {
      controller.abort();
    }, timeoutMs);
  }

  try {
    const res = await fetch(path, {
      headers: { 'Content-Type':'application/json' },
      ...fetchOptions,
      signal: controller.signal,
    });
    const text = await res.text();
    let data = {};
    try { data = JSON.parse(text); } catch(e) { data = { raw:text }; }
    if (!res.ok) {
      const detail = data && Object.prototype.hasOwnProperty.call(data, 'detail') ? data.detail : null;
      const message =
        typeof detail === 'string' ? detail
        : detail ? JSON.stringify(detail)
        : (typeof data.error === 'string' ? data.error : (text || ('HTTP ' + res.status)));
      throw new Error(message);
    }
    return data;
  } catch (e) {
    if (e && e.name === 'AbortError') {
      const sec = timeoutMs > 0 ? Math.round(timeoutMs / 1000) : 30;
      throw new Error(`Превышено время ожидания (${sec}с). Попробуйте ещё раз.`);
    }
    throw e;
  } finally {
    if (timeoutId) {
      window.clearTimeout(timeoutId);
    }
  }
}

function setSection(name){
  if (IS_SCORING_STANDALONE && !['scoring', 'scoring_creatives', 'scoring_templates'].includes(name)) {
    return;
  }
  if (IS_AUDITS_STANDALONE && name !== 'audits') {
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
  if (name === 'crm') {
    loadCrmDataAndRender();
  }
  if (name === 'audits') {
    loadAuditsDataAndRender();
  }
}

const BUTTON_HELP_HINTS = Object.freeze({
  'Обновить данные': 'Повторно загружает данные текущей страницы с сервера.',
  'Пересчитать скоринг': 'Пересобирает признаки и заново считает score/сегменты посетителей.',
  'Применить': 'Применяет выбранные фильтры к данным на экране.',
  'Обновить': 'Перезагружает блок с актуальными данными.',
  'Открыть': 'Открывает детальную карточку записи.',
  'Сгенерировать баннеры': 'Запускает генерацию баннеров для выбранной аудитории.',
  'Запустить sync в Директ': 'Проверяет и синхронизирует аудитории в Яндекс Директ.',
  'Запустить диагностику': 'Выполняет проверку интеграций, API и ключевых сервисов.',
  'Скопировать отчёт': 'Копирует текст текущего диагностического отчёта.',
  'Синхронизировать из SERM': 'Запускает прямую синхронизацию из SERM и загрузку клиентов/статусов/коммуникаций.',
  'Показать коммуникации': 'Открывает историю коммуникаций по выбранному клиенту.',
  'Тестовый запуск аудита': 'Создаёт тестовый run и запускает worker OpenRouter, чтобы проверить полный контур аудита.',
  'Принять': 'Подтверждает результат аудита как достаточный.',
  'Нужна доработка': 'Переводит результат аудита в режим обязательной доработки.',
  'Отклонить': 'Отклоняет результат аудита и фиксирует решение.',
});

function normalizeHelpText(value){
  return String(value || '').replace(/\s+/g, ' ').trim();
}

const NAV_ICON_MAP = Object.freeze({
  'Обзор': { lib: 'lucide', name: 'house' },
  'Кампании и объявления': { lib: 'lucide', name: 'megaphone' },
  'Структура кампаний': { lib: 'lucide', name: 'blocks' },
  'Минус-слова': { lib: 'lucide', name: 'circle-off' },
  'Прогноз': { lib: 'lucide', name: 'chart-column' },
  'Скоринг посетителей': { lib: 'lucide', name: 'target' },
  'Аудитории и активация': { lib: 'lucide', name: 'users' },
  'Шаблоны баннеров': { lib: 'lucide', name: 'image' },
  'CRM / AlfaCRM': { lib: 'tabler', name: 'users-group' },
  'Аудит OpenRouter': { lib: 'tabler', name: 'brand-openai' },
  'Журнал действий': { lib: 'lucide', name: 'clipboard-list' },
  'Диагностика': { lib: 'lucide', name: 'stethoscope' },
});

const BUTTON_ICON_MAP = Object.freeze({
  'Компактная панель': { lib: 'lucide', name: 'layout-dashboard' },
  'Обновить данные': { lib: 'lucide', name: 'refresh-cw' },
  'Проверка системы': { lib: 'lucide', name: 'stethoscope' },
  'Открыть обзор': { lib: 'lucide', name: 'house' },
  'Пересчитать скоринг': { lib: 'lucide', name: 'calculator' },
  'Сгенерировать гипотезы': { lib: 'lucide', name: 'lightbulb' },
  'Применить': { lib: 'lucide', name: 'check' },
  'Обновить': { lib: 'lucide', name: 'refresh-cw' },
  'Открыть': { lib: 'lucide', name: 'external-link' },
  'Закрыть': { lib: 'lucide', name: 'x' },
  'Сгенерировать баннеры': { lib: 'lucide', name: 'image-plus' },
  'Запустить sync в Директ': { lib: 'tabler', name: 'brand-yandex' },
  'Запустить диагностику': { lib: 'lucide', name: 'stethoscope' },
  'Скопировать отчёт': { lib: 'lucide', name: 'copy' },
  'Синхронизировать из SERM': { lib: 'lucide', name: 'refresh-cw' },
  'Показать коммуникации': { lib: 'lucide', name: 'message-square' },
  'Тестовый запуск аудита': { lib: 'lucide', name: 'flask-conical' },
  'Принять': { lib: 'lucide', name: 'circle-check' },
  'Нужна доработка': { lib: 'lucide', name: 'hammer' },
  'Отклонить': { lib: 'lucide', name: 'circle-x' },
});

function buildIconMarkup(spec){
  if (!spec || !spec.name) return '';
  if (spec.lib === 'tabler') {
    return `<i class="ti ti-${esc(spec.name)}" aria-hidden="true"></i>`;
  }
  return `<i data-lucide="${esc(spec.name)}" aria-hidden="true"></i>`;
}

function renderIconLibraries(){
  if (window.lucide && typeof window.lucide.createIcons === 'function') {
    window.lucide.createIcons();
  }
}

function decorateNavIcons(){
  document.querySelectorAll('.nav button').forEach((btn) => {
    const labelNode = btn.querySelector('.nav-label');
    const iconNode = btn.querySelector('.nav-icon');
    if (!labelNode || !iconNode) return;
    const label = normalizeHelpText(labelNode.textContent);
    const spec = NAV_ICON_MAP[label];
    if (!spec) return;
    const key = `${spec.lib}:${spec.name}`;
    if (iconNode.dataset.iconKey === key) return;
    iconNode.dataset.iconKey = key;
    iconNode.innerHTML = buildIconMarkup(spec);
  });
}

function buttonBaseText(btn){
  const clone = btn.cloneNode(true);
  clone.querySelectorAll('.btn-help, .btn-icon').forEach((n) => n.remove());
  return normalizeHelpText(clone.textContent);
}

function decorateButtonIcons(){
  document.querySelectorAll('.main .btn').forEach((btn) => {
    if (btn.dataset.iconLocked === '1') return;
    const baseText = buttonBaseText(btn);
    const spec = BUTTON_ICON_MAP[baseText];
    if (!spec) return;
    const iconKey = `${spec.lib}:${spec.name}`;
    if (btn.dataset.iconKey === iconKey) return;
    const old = btn.querySelector(':scope > .btn-icon');
    if (old) old.remove();

    const iconWrap = document.createElement('span');
    iconWrap.className = 'btn-icon';
    iconWrap.setAttribute('aria-hidden', 'true');
    iconWrap.innerHTML = buildIconMarkup(spec);
    btn.prepend(iconWrap);
    btn.dataset.iconKey = iconKey;
  });
}

function decorateUiHelpHints(){
  const nodes = document.querySelectorAll('.main .btn');
  nodes.forEach((btn) => {
    if (btn.classList.contains('btn-help')) return;
    if (btn.querySelector(':scope > .btn-help')) return;
    if (btn.dataset.noHelp === '1') return;

    const text = normalizeHelpText(btn.textContent);
    const explicit = normalizeHelpText(btn.dataset.help || btn.getAttribute('title'));
    const hint = BUTTON_HELP_HINTS[text] || explicit || text;
    if (!hint) return;

    btn.classList.add('btn-with-help');
    btn.dataset.help = hint;
    if (!btn.getAttribute('title')) btn.setAttribute('title', hint);

    const help = document.createElement('span');
    help.className = 'btn-help';
    help.textContent = 'i';
    help.setAttribute('data-help', hint);
    help.setAttribute('aria-hidden', 'true');
    btn.appendChild(help);
  });
}

function confidenceBadge(value){
  const v = (value || 'LOW').toUpperCase();
  if (v === 'HIGH') return '<span class="badge good">HIGH</span>';
  if (v === 'MEDIUM') return '<span class="badge warn">MEDIUM</span>';
  return '<span class="badge">LOW</span>';
}

function helpDot(text){
  const hint = esc(text || '');
  return `<span class="help-dot" tabindex="0" data-help="${hint}">i</span>`;
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

function escapeJsSingleQuoted(value){
  return String(value || '')
    .replaceAll('\\', '\\\\')
    .replaceAll("'", "\\'");
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
    return_visitor: 'повторный посетитель',
    high_intent_source: 'высокоинтентный источник',
    bounce_session: 'отказный визит'
  };
  return labels[key] || key;
}

function sourceLabel(value){
  const v = String(value || '').trim().toLowerCase();
  const map = {
    yandex_direct: 'Яндекс Директ',
    vk_ads: 'VK Реклама',
    direct: 'Прямые заходы',
    organic: 'Органический поиск',
    referral: 'Реферальный трафик',
    social: 'Соцсети',
    messenger: 'Мессенджеры',
    email: 'Электронная почта',
    ad: 'Реклама',
    internal: 'Внутренние переходы',
    unknown: 'Не определено',
    '': 'Не определено',
  };
  if (map[v]) return map[v];
  if (v.includes('gmail')) return 'Электронная почта (Gmail)';
  if (v.includes('mail')) return 'Электронная почта';
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
      plugins: { legend: { labels: { color: '#334155' } } },
      scales: {
        x: { ticks: { color: '#64748b', maxRotation: 0 }, grid: { color: 'rgba(148,163,184,.35)' } },
        y: { ticks: { color: '#64748b' }, grid: { color: 'rgba(148,163,184,.35)' }, beginAtZero: true },
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
          borderColor: ['#ffffff', '#ffffff', '#ffffff'],
          borderWidth: 1,
        },
      ],
    },
    options: {
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#334155' } } },
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
    <div class="card"><div class="metric-label">Прогнозные проверки</div><div class="metric-value">${esc(s.forecast_items ?? 0)}</div></div>
    <div class="card"><div class="metric-label">Одобрено</div><div class="metric-value">${esc(s.approved_actions ?? 0)}</div></div>
    <div class="card"><div class="metric-label">Ожидают решения</div><div class="metric-value">${esc(s.pending_actions ?? 0)}</div></div>
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
                <div class="small muted">ID объявления: ${esc(r.ad_id)} · Группа: ${esc(r.ad_group_id)}</div>
              </div>
              ${confidenceBadge(r.prediction_confidence)}
            </div>
            <div class="row" style="margin-top:10px">
              <span class="badge">Скор ${esc(r.score)}</span>
              <span class="badge">CTR ${esc(r.ctr_pct)}%</span>
              <span class="badge">Прогноз CTR ${esc(r.predicted_ctr_pct || '-')}%</span>
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
                <div class="small muted">Группа: ${esc(r.ad_group_id)}</div>
              <div class="row" style="margin-top:10px">
                ${statusBadge(r.action_status)}
                <button class="btn primary" onclick="structureAction(${JSON.stringify(r.campaign_name)}, ${r.ad_group_id}, 'APPLY_SPLIT')">Разделить группу</button>
                <button class="btn ghost" onclick="copyContext('ST-${r.ad_group_id}')">Скопировать для AI</button>
              </div>
            </div>
          `).join('') : `<div class="empty">Нет структурных проблем</div>`}
        </div>

        <div class="panel">
          <div class="panel-title">Проверка прогноза</div>
          ${forecast.length ? forecast.map(r => `
            <div class="panel" style="padding:12px;margin-bottom:10px">
              <div><b>${esc(r.campaign_name)}</b></div>
              <div class="small muted">ID объявления: ${esc(r.ad_id)} · ${esc(r.forecast_status || '-')}</div>
              <div class="row" style="margin-top:10px">
                <span class="badge">Прогноз CTR ${esc(r.predicted_ctr_pct || '-')}</span>
                <span class="badge">Факт CTR ${esc(r.actual_ctr_pct || '-')}</span>
                <span class="badge">Прогноз CPC ${esc(r.predicted_cpc || '-')}</span>
                <span class="badge">Факт CPC ${esc(r.actual_cpc || '-')}</span>
              </div>
            </div>
          `).join('') : `<div class="empty">Нет данных проверки прогноза</div>`}
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
            <th>Объявление / Группа</th>
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
                ID объявления: ${esc(r.ad_id)}<br>
                Группа: ${esc(r.ad_group_id)}
              </td>
              <td>
                <div><b>T1:</b> ${esc(r.original_title || '')}</div>
                <div><b>T2:</b> ${esc(r.original_title_2 || '')}</div>
                <div><b>Текст:</b> ${esc(r.original_body_text || '')}</div>
                <div class="chips">
                  ${(String(r.sample_queries || '').split('|').map(x=>x.trim()).filter(Boolean).slice(0,8)).map(q => `<span class="chip">${esc(q)}</span>`).join('')}
                </div>
              </td>
              <td>
                <div>Скор: <b>${esc(r.score)}</b></div>
                <div>CTR: <b>${esc(r.ctr_pct)}%</b></div>
                <div>CPC: <b>${esc(r.cpc)}</b></div>
                <div>Прогноз CTR: <b>${esc(r.predicted_ctr_pct || '-')}</b></div>
                <div>Прогноз CPC: <b>${esc(r.predicted_cpc || '-')}</b></div>
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
            <option value="hot">Горячие</option>
            <option value="warm">Тёплые</option>
            <option value="cold">Холодные</option>
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
      <div class="small muted" style="margin-top:6px">Можно использовать как основу для объявлений в Директ и сегментных ретаргет-кампаний.</div>
    </div>
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Аудитории и активация в Директ</div>
        <div class="small muted">окно ${esc(activationPlan.days || cohorts.days || 90)} дней</div>
      </div>
      <div class="row" style="margin-top:8px">
        <button class="btn ghost" onclick="bootstrapScoringDirect(false)">Подготовить сущности в Директ (проверка)</button>
        <button class="btn ghost" onclick="bootstrapScoringDirect(true)">Создать сущности в Директ</button>
        <button class="btn" onclick="syncScoringDirect(true)">Проверка синхронизации</button>
        <button class="btn primary" onclick="syncScoringDirect(false)">Синхронизировать в Директ</button>
      </div>
      <div class="small muted" style="margin-top:10px">
        Аудиторий: ${esc(activationPlan.count || 0)} · Готовы к активации: ${esc(activationPlan.eligible_count || 0)} · Минимальный размер: ${esc(activationPlan.min_audience_size || 100)}
      </div>
      <div class="table-wrap">
        <table class="table">
          <thead>
            <tr>
              <th>Аудитория</th>
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
            `).join('') : `<tr><td colspan="7"><div class="empty">Нет данных по аудиториям</div></td></tr>`}
          </tbody>
        </table>
      </div>
      <div class="code" style="margin-top:10px">Пример выгрузки: /api/scoring/audiences/export?days=90&segment=warm&os_root=android&limit=5000</div>
    </div>
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Реакция в Директ по scoring-тегам</div>
        <div class="small muted">${esc(reaction.count || 0)} тегов · ${esc(reaction.days || 30)} дней</div>
      </div>
      <div class="small muted" style="margin-top:6px">
        Показы: ${esc((reaction.totals || {}).impressions || 0)} · Клики: ${esc((reaction.totals || {}).clicks || 0)} · Расход: ${esc((reaction.totals || {}).cost || 0)}
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
  const totalAudience = items.reduce((acc, row) => acc + Number(row.audience_size || 0), 0);
  const avgAudience = items.length ? Math.round(totalAudience / items.length) : 0;

  document.getElementById('section-scoring_templates').innerHTML = `
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Шаблоны баннеров по сегментам</div>
        <div class="small muted">аудиторий: ${esc(payload.count || 0)} · вариантов: ${esc(totalVariants)} · окно: ${esc(days)} дней</div>
      </div>
      <div class="small muted" style="margin-top:6px">Выберите аудиторию, проверьте гипотезу и сгенерируйте баннеры под нужный сегмент.</div>
      <div class="template-kpi-grid">
        <div class="template-kpi"><div class="label">Период данных</div><div class="value">${esc(days)} дн.</div></div>
        <div class="template-kpi"><div class="label">Аудиторий</div><div class="value">${esc(items.length)}</div></div>
        <div class="template-kpi"><div class="label">Вариантов</div><div class="value">${esc(totalVariants)}</div></div>
        <div class="template-kpi"><div class="label">Средний размер аудитории</div><div class="value">${esc(avgAudience)}</div></div>
      </div>
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
            <option value="1" ${minAudience === 1 ? 'selected' : ''}>мин. 1</option>
            <option value="50" ${minAudience === 50 ? 'selected' : ''}>мин. 50</option>
            <option value="100" ${minAudience === 100 ? 'selected' : ''}>мин. 100</option>
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
      ${isReady ? '' : `<div class="code" style="margin-top:10px">API шаблонов недоступен: ${esc(payload.error || 'неизвестная ошибка')}</div>`}
    </div>

    ${items.length ? items.map((row) => {
      const safeId = safeDomId(row.cohort_name || '');
      return `
      <div class="panel template-cohort-shell">
        <div class="template-cohort-main">
          <div class="row" style="justify-content:space-between;align-items:flex-start">
            <div>
              <div><b>${esc(row.cohort_name || '-')}</b> · ${segmentBadge(row.segment)}</div>
              <div class="small muted">
                Аудитория: ${esc(row.audience_size || 0)} · Окно: ${esc(row.window_days || days)} дн. · ОС: ${esc((row.os_root || 'all').toUpperCase())}
              </div>
            </div>
            <div class="small muted">Тег Директ: <span class="badge">${esc(row.direct_tag || '-')}</span></div>
          </div>

          <div class="grid-2 template-cohort-meta">
            <div class="card">
              <div class="metric-label">Кратко по сегменту</div>
              <div class="code">Причина: ${esc(reasonLabel(row.short_reason_hint || 'unknown'))}
Источник: ${esc(sourceLabel(row.source_hint || 'unknown'))}
Сегмент: ${esc(String(row.segment || '-').toUpperCase())}
Гипотеза: ${esc(shortText(row.kpi_hypothesis?.objective || '-', 140))}</div>
            </div>
            <div class="card">
              <div class="metric-label">Связка с Директ</div>
              <div class="code">ad_group_id: ${esc(row.ad_group_id || '-')}
retargeting_list_id: ${esc(row.retargeting_list_id || '-')}
goal_id: ${esc(row.goal_id || '-')}
Приоритет: ${esc(row.strategy_priority || '-')}</div>
            </div>
          </div>

          <details class="template-details">
            <summary>KPI-гипотеза (раскрыть детали)</summary>
            <div class="template-details-content">
              <div class="code">Цель: ${esc(row.kpi_hypothesis?.objective || '-')}
Окно сравнения: ${esc(row.kpi_hypothesis?.comparison_window_days || '-')}д

Экономика:
- Средний чек: ${esc(numText(row.kpi_hypothesis?.economics?.avg_check_rub, 0))} ₽
- Маржа: ${esc(numText(row.kpi_hypothesis?.economics?.margin_pct, 0))}% (${esc(numText(row.kpi_hypothesis?.economics?.margin_rub, 0))} ₽)
- Доля CAC от маржи (потолок): ${esc(numText(row.kpi_hypothesis?.economics?.max_marketing_share_of_margin_pct, 1))}%

Базовые факт-метрики (если уже есть реакция):
- Показы: ${esc(row.kpi_hypothesis?.baseline?.impressions ?? '-')}
- Клики: ${esc(row.kpi_hypothesis?.baseline?.clicks ?? '-')}
- CTR(STR): ${esc(numText(row.kpi_hypothesis?.baseline?.ctr_pct, 2))}%
- CPC: ${esc(numText(row.kpi_hypothesis?.baseline?.avg_cpc_rub, 2))} ₽
- Расход: ${esc(numText(row.kpi_hypothesis?.baseline?.cost_rub, 2))} ₽

Что ожидаем:
- CTR(STR): >= ${esc(numText(row.kpi_hypothesis?.expected?.ctr_str_min_pct, 2))}% (цель ${esc(numText(row.kpi_hypothesis?.expected?.ctr_str_target_pct, 2))}%)
- CR клик->заявка: >= ${esc(numText(row.kpi_hypothesis?.expected?.click_to_lead_min_pct ?? row.kpi_hypothesis?.expected?.cvr_to_lead_min_pct, 2))}% (цель ${esc(numText(row.kpi_hypothesis?.expected?.click_to_lead_target_pct ?? row.kpi_hypothesis?.expected?.cvr_to_lead_target_pct, 2))}%)
- Факт CR клик->заявка (${esc(row.kpi_hypothesis?.expected?.reference_window_days ?? row.conversion_reference?.selected_window_days ?? '-') }д): ${esc(numText(row.kpi_hypothesis?.expected?.click_to_lead_actual_pct, 2))}% (${esc(row.kpi_hypothesis?.expected?.click_to_lead_basis || 'model')})
- Цена клика (CPC): цель <= ${esc(numText(row.kpi_hypothesis?.expected?.target_cpc_rub, 2))} ₽ (макс ${esc(numText(row.kpi_hypothesis?.expected?.max_cpc_rub ?? row.kpi_hypothesis?.expected?.avg_cpc_max_rub, 2))} ₽)
- Цена заявки (CPL): цель <= ${esc(numText(row.kpi_hypothesis?.expected?.target_cpl_rub, 2))} ₽ (макс ${esc(numText(row.kpi_hypothesis?.expected?.max_cpl_rub, 2))} ₽)
- Цена оплаты (CAC): цель <= ${esc(numText(row.kpi_hypothesis?.expected?.target_cac_pay_rub ?? row.kpi_hypothesis?.expected?.target_cpa_rub, 2))} ₽ (макс ${esc(numText(row.kpi_hypothesis?.expected?.max_cac_pay_rub, 2))} ₽)
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
          </details>

          <details class="template-details" open>
            <summary>Варианты сообщений для этой аудитории</summary>
            <div class="template-details-content">
              <div class="table-wrap">
                <table class="table">
                  <thead>
                    <tr>
                      <th>Вариант</th>
                      <th>Креативный угол</th>
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
                    `).join('') : `<tr><td colspan="6"><div class="empty">Нет вариантов для этой аудитории</div></td></tr>`}
                  </tbody>
                </table>
              </div>
            </div>
          </details>
        </div>

        <aside class="template-banner-side">
          <div class="template-banner-side-head">
            <div class="metric-label">Сгенерированные баннеры</div>
            <div id="banner-inline-status-${safeId}" class="small muted"></div>
          </div>
          <button id="banner-generate-btn-${safeId}" class="btn primary template-banner-side-btn" onclick="generateScoringBanners('${escapeJsSingleQuoted(row.cohort_name || '')}')">
            Сгенерировать баннеры
          </button>
          <div id="banners-${safeId}" class="template-banner-content">
            ${renderTemplateBannerSlots([])}
          </div>
        </aside>
      </div>
    `;
    }).join('') : `
      <div class="panel">
        <div class="empty">Нет данных для шаблонов баннеров. Проверьте скоринг и план активации.</div>
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
    : { ready: false, error: summaryRes.reason?.message || 'ошибка API summary' };
  const visitors = visitorsRes.status === 'fulfilled'
    ? visitorsRes.value
    : { ready: false, items: [], count: 0, limit: SCORING_FILTERS.limit || 100, error: visitorsRes.reason?.message || 'ошибка API visitors' };
  const timeseries = timeseriesRes.status === 'fulfilled'
    ? timeseriesRes.value
    : { ready: false, dates: [], hot: [], warm: [], cold: [], error: timeseriesRes.reason?.message || 'ошибка API timeseries' };
  const audience = audienceRes.status === 'fulfilled'
    ? audienceRes.value
    : { ready: false, gender_age: [], source_mix: [], device_mix: [], error: audienceRes.reason?.message || 'ошибка API audience' };
  const attribution = attributionRes.status === 'fulfilled'
    ? attributionRes.value
    : { ready: false, status: 'error', direct_pct: 0, unknown_pct: 0, top_sources: [], error: attributionRes.reason?.message || 'ошибка API attribution' };
  const creativePlan = creativePlanRes.status === 'fulfilled'
    ? creativePlanRes.value
    : { ready: false, items: [], count: 0, days: 90, error: creativePlanRes.reason?.message || 'ошибка API creative plan' };
  const cohorts = cohortsRes.status === 'fulfilled'
    ? cohortsRes.value
    : { ready: false, cohorts: [], matrix: [], days: 90, error: cohortsRes.reason?.message || 'ошибка API cohorts' };
  const activationPlan = activationPlanRes.status === 'fulfilled'
    ? activationPlanRes.value
    : { ready: false, cohorts: [], count: 0, eligible_count: 0, min_audience_size: 100, error: activationPlanRes.reason?.message || 'ошибка API activation plan' };
  const reaction = reactionRes.status === 'fulfilled'
    ? reactionRes.value
    : { ready: false, items: [], count: 0, totals: { impressions: 0, clicks: 0, cost: 0 }, error: reactionRes.reason?.message || 'ошибка API activation reaction' };
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
      error: adTemplatesRes.reason?.message || 'ошибка API ad templates',
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

function setBannerGenerateButtonState(cohortName, busy=false, busyLabel='Генерация...'){
  const safeId = safeDomId(cohortName || '');
  const button = document.getElementById(`banner-generate-btn-${safeId}`);
  if (!button) return;
  if (!button.dataset.defaultText) {
    button.dataset.defaultText = button.textContent || 'Сгенерировать баннеры';
  }
  button.disabled = Boolean(busy);
  button.classList.toggle('btn-loading', Boolean(busy));
  button.textContent = busy ? String(busyLabel || 'Генерация...') : button.dataset.defaultText;
}

function setBannerInlineStatus(cohortName, text='', mode='muted'){
  const safeId = safeDomId(cohortName || '');
  const node = document.getElementById(`banner-inline-status-${safeId}`);
  if (!node) return;
  node.classList.remove('status-ok', 'status-warn', 'status-error');
  if (mode === 'ok') node.classList.add('status-ok');
  if (mode === 'warn') node.classList.add('status-warn');
  if (mode === 'error') node.classList.add('status-error');
  node.textContent = String(text || '');
}

function renderBannerProgressState(container, safeId, percent, message){
  const pct = Math.max(0, Math.min(100, Number(percent || 0)));
  container.innerHTML = `
    <div class="banner-progress">
      <div class="banner-progress-head">
        <span class="small">Генерация баннера</span>
        <span id="banner-progress-pct-${safeId}" class="small muted">${esc(Math.round(pct))}%</span>
      </div>
      <div class="banner-progress-track">
        <div id="banner-progress-fill-${safeId}" class="banner-progress-fill" style="width:${esc(pct)}%"></div>
      </div>
      <div id="banner-progress-text-${safeId}" class="small muted">${esc(message || 'Выполняется запрос...')}</div>
    </div>
  `;
}

function updateBannerProgressState(safeId, percent, message){
  const pct = Math.max(0, Math.min(100, Number(percent || 0)));
  const fill = document.getElementById(`banner-progress-fill-${safeId}`);
  const pctText = document.getElementById(`banner-progress-pct-${safeId}`);
  const msg = document.getElementById(`banner-progress-text-${safeId}`);
  if (fill) fill.style.width = `${pct}%`;
  if (pctText) pctText.textContent = `${Math.round(pct)}%`;
  if (msg && message) msg.textContent = String(message);
}

function renderTemplateBannerSlots(images){
  const list = Array.isArray(images) ? images.slice(0, 3) : [];
  const slots = list.map((img, idx) => `
    <div class="template-banner-slot">
      <a href="${esc(img.static_url)}" target="_blank" rel="noopener noreferrer">
        <img class="banner-image" src="${esc(img.static_url)}" alt="${esc(img.variant_key || `banner-${idx + 1}`)}"/>
      </a>
      <div class="small" style="margin-top:6px"><b>${esc(img.variant_key || `v${idx + 1}`)}</b></div>
      <div class="small muted">${esc(shortText(img.headline || '', 64))}</div>
    </div>
  `);
  for (let idx = list.length; idx < 3; idx += 1) {
    slots.push(`
      <div class="template-banner-slot placeholder">
        <div class="template-banner-placeholder-index">${idx + 1}</div>
        <div class="small muted">Баннер пока не сгенерирован</div>
      </div>
    `);
  }
  return `<div class="template-banner-rail">${slots.join('')}</div>`;
}

async function generateScoringBanners(cohortName){
  const name = String(cohortName || '').trim();
  if (!name) return;
  const safeId = safeDomId(name);
  const existingJob = BANNER_GENERATION_JOBS[safeId];
  if (existingJob && existingJob.busy) return;

  const containerId = 'banners-' + safeId;
  const container = document.getElementById(containerId);
  setBannerGenerateButtonState(name, true, 'Генерируем...');
  setBannerInlineStatus(name, 'Запуск генерации...', 'warn');

  let progress = 7;
  let timer = null;
  let longWaitTimer = null;
  if (container) {
    container.scrollIntoView({ behavior: 'smooth', block: 'center' });
    renderBannerProgressState(container, safeId, progress, 'Подготовка запроса...');
    timer = window.setInterval(() => {
      progress = Math.min(progress + (progress < 65 ? 7 : progress < 88 ? 3 : 1), 92);
      const label = progress < 32
        ? 'Формируем промпт...'
        : progress < 66
          ? 'Генерируем изображение...'
          : 'Сохраняем баннер...';
      setBannerInlineStatus(name, label, 'warn');
      updateBannerProgressState(safeId, progress, label);
    }, 650);
    longWaitTimer = window.setTimeout(() => {
      setBannerInlineStatus(name, 'Дольше обычного (до 1–2 минут)...', 'warn');
      updateBannerProgressState(
        safeId,
        Math.max(progress, 92),
        'Запрос выполняется дольше обычного. Обычно это до 1–2 минут...'
      );
    }, 15000);
  }
  BANNER_GENERATION_JOBS[safeId] = { busy: true, timer, longWaitTimer };

  const tpl = DASH.scoring_ad_templates || {};
  try {
    const data = await api('/api/scoring/ad-templates/generate-banners', {
      method: 'POST',
      timeoutMs: 120000,
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

    if (timer) window.clearInterval(timer);
    if (longWaitTimer) window.clearTimeout(longWaitTimer);
    delete BANNER_GENERATION_JOBS[safeId];
    setBannerGenerateButtonState(name, false);

    const images = data.generated || [];
    setBannerInlineStatus(name, `Готово: ${Number(data.generated_count || images.length)} шт.`, 'ok');
    if (!container) return;
    if (!images.length) {
      setBannerInlineStatus(name, 'Завершено без изображений', 'warn');
      container.innerHTML = `
        <div class="template-banner-meta small muted">Генерация завершена, но изображения не получены.</div>
        ${renderTemplateBannerSlots([])}
      `;
      return;
    }
    const costText = Number.isFinite(Number(data.cost_usd))
      ? ` · Стоимость: ~$${esc(numText(Number(data.cost_usd), 4))}`
      : '';
    const usage = data.usage || {};
    const usageText = Number(usage.total_tokens || 0) > 0
      ? ` · Токены: ${esc(numText(Number(usage.total_tokens || 0), 0))}`
      : '';

    container.innerHTML = `
      <div class="template-banner-meta small muted">
        Сгенерировано: ${esc(data.generated_count || images.length)} · Провайдер: ${esc(data.provider_used || data.provider_requested || '-')} · Модель: ${esc(data.model_used || data.model || '-')} · Размер: ${esc(data.size || '-')}${costText}${usageText}
      </div>
      ${renderTemplateBannerSlots(images)}
      ${(data.failed_count || 0) > 0 ? `<div class="code" style="margin-top:8px">Ошибок генерации: ${esc(data.failed_count)}</div>` : ''}
    `;
  } catch (e) {
    if (timer) window.clearInterval(timer);
    if (longWaitTimer) window.clearTimeout(longWaitTimer);
    delete BANNER_GENERATION_JOBS[safeId];
    setBannerGenerateButtonState(name, false);
    setBannerInlineStatus(name, 'Ошибка генерации', 'error');
    if (container) {
      renderBannerProgressState(container, safeId, Math.max(progress, 12), 'Остановлено');
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
    const ok = confirm('Выполнить запись в API Директ? Проверьте SCORING_DIRECT_SYNC_ENABLED и сопоставления ID.');
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
      `Синхронизация в Директ: ${data.dry_run ? 'проверка' : 'выполнение'}\n` +
      `Готово к активации: ${data.eligible_count || 0}\n` +
      `Попыток: ${sync.attempted || 0}\n` +
      `Применено: ${sync.applied || 0}\n` +
      `Пропущено: ${sync.skipped || 0}\n` +
      `Ошибок: ${sync.errors || 0}`
    );
    await loadScoringDataAndRender();
  } catch (e) {
    alert('Ошибка синхронизации в Директ: ' + e.message);
  }
}

async function bootstrapScoringDirect(apply=false){
  if (apply) {
    const ok = confirm('Создать сущности в Директ и записать ID в .env?');
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
      `Подготовка сущностей Директ: ${data.apply ? 'создание' : 'проверка'}\n` +
      `campaign_id: ${data.campaign_id || '-'}\n` +
      `Выбрано аудиторий: ${data.cohorts_selected || 0}\n` +
      `Создано групп: ${b.created_adgroups || 0}\n` +
      `Создано списков: ${b.created_retargeting_lists || 0}\n` +
      `Подключено таргетов: ${b.attached_audience_targets || 0}\n` +
      `Ошибок: ${b.errors || 0}`
    );
    await loadScoringDataAndRender();
  } catch (e) {
    alert('Ошибка подготовки сущностей Директ: ' + e.message);
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
      <div class="panel-title">Посетитель ${esc(data.visitor_id || '')}</div>
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
      <div class="code">- источник: ${esc(sourceText || '-')}
- режим источника: ${esc(sourceMode)}
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

function crmSegmentLabel(value){
  const v = String(value || '').trim();
  const map = {
    customers_all: 'Все клиенты',
    leads_active: 'Лиды активные',
    leads_archived: 'Лиды архив',
    clients_active: 'Клиенты активные',
    clients_archived: 'Клиенты архив',
  };
  return map[v] || v || '-';
}

function crmBoolLabel(value){
  if (value === null || value === undefined || value === '') return '-';
  return Number(value) ? 'Да' : 'Нет';
}

function crmPromptDate(defaultValue=''){
  const raw = prompt('Введите report_date (YYYY-MM-DD)', defaultValue || new Date().toISOString().slice(0, 10));
  if (raw === null) return null;
  const text = String(raw).trim();
  if (!text) return '';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    alert('Неверный формат даты. Используйте YYYY-MM-DD');
    return null;
  }
  return text;
}

function renderCrm(){
  const status = DASH.crm_load_status || {};
  const users = DASH.crm_users || {};
  const comms = DASH.crm_communications || {};
  const meta = DASH.crm_meta || {};
  const latest = (status.items || [])[0] || null;
  const loadHistory = status.items || [];
  const totalUsersLoaded = loadHistory.reduce((acc, x) => acc + Number(x?.users_rows || 0), 0);
  const totalCommsLoaded = loadHistory.reduce((acc, x) => acc + Number(x?.communications_rows || 0), 0);
  const selectedCustomerText = meta.selected_customer_id
    ? `${meta.selected_customer_id}${meta.selected_customer_name ? ` · ${meta.selected_customer_name}` : ''}`
    : '-';

  document.getElementById('section-crm').innerHTML = `
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">CRM / AlfaCRM</div>
        <div class="row">
          <button class="btn" onclick="loadCrmDataAndRender()">Обновить данные</button>
          <button class="btn primary" onclick="runCrmDirectSync()">Синхронизировать из SERM</button>
        </div>
      </div>
      <div class="small muted" style="margin-top:8px">
        Последняя загрузка:
        ${latest ? `файл ${esc(latest.source_file || '-')} · дата ${esc(latest.report_date || '-')} · users ${esc(latest.users_rows || 0)} · communications ${esc(latest.communications_rows || 0)} · режим ${esc(latest.note || '-')} · loaded_at ${esc(latest.loaded_at || '-')}` : 'нет данных'}
      </div>
      <div class="small muted" style="margin-top:6px">
        История (до ${esc(status.limit || 20)}): ${esc(loadHistory.length)} записей · users всего: ${esc(totalUsersLoaded)} · communications всего: ${esc(totalCommsLoaded)}
      </div>
      <div class="row scoring-filter-row mt-1">
        <div class="col-md-3 col-sm-6"><label><input type="checkbox" id="crm-sync-updates-only" ${CRM_SYNC_OPTIONS.updates_only ? 'checked' : ''}/> Только обновления</label></div>
        <div class="col-md-3 col-sm-6"><label><input type="checkbox" id="crm-sync-include-comms" ${CRM_SYNC_OPTIONS.include_communications ? 'checked' : ''}/> Коммуникации</label></div>
        <div class="col-md-3 col-sm-6"><label><input type="checkbox" id="crm-sync-include-lessons" ${CRM_SYNC_OPTIONS.include_lessons ? 'checked' : ''}/> Длинные разделы (уроки)</label></div>
        <div class="col-md-3 col-sm-6"><label><input type="checkbox" id="crm-sync-include-extra" ${CRM_SYNC_OPTIONS.include_extra ? 'checked' : ''}/> Прочие длинные разделы</label></div>
      </div>
      <div class="row scoring-filter-row mt-1">
        <div class="col-md-2 col-sm-6">
          <input id="crm-sync-timeout-sec" class="form-control" type="number" min="60" max="7200" value="${esc(CRM_SYNC_OPTIONS.timeout_sec || 1800)}" />
        </div>
        <div class="col-md-10 col-sm-6 small muted">Основной контур: клиенты + статусы оплаты + коммуникации. Длинные разделы по умолчанию отключены.</div>
      </div>
      ${status.ready === false && status.error ? `<div class="code" style="margin-top:10px">${esc(status.error)}</div>` : ''}
    </div>

    <div class="panel">
      <div class="panel-title">История загрузок AlfaCRM</div>
      <div class="table-wrap" style="margin-top:10px">
        <table class="table">
          <thead>
            <tr>
              <th>loaded_at</th>
              <th>report_date</th>
              <th>source_file</th>
              <th>users_rows</th>
              <th>communications_rows</th>
              <th>note</th>
            </tr>
          </thead>
          <tbody>
            ${loadHistory.length ? loadHistory.map((r) => `
              <tr>
                <td>${esc(r.loaded_at || '-')}</td>
                <td>${esc(r.report_date || '-')}</td>
                <td>${esc(r.source_file || '-')}</td>
                <td>${esc(r.users_rows || 0)}</td>
                <td>${esc(r.communications_rows || 0)}</td>
                <td>${esc(r.note || '-')}</td>
              </tr>
            `).join('') : `<tr><td colspan="6"><div class="empty">История загрузок отсутствует. Нажмите «Синхронизировать из SERM».</div></td></tr>`}
          </tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <div class="row scoring-filter-row mt-1">
        <div class="col-md-2 col-sm-6">
          <input id="crm-report-date-filter" class="form-control" placeholder="YYYY-MM-DD" value="${esc(CRM_FILTERS.report_date || '')}"/>
        </div>
        <div class="col-md-3 col-sm-6">
          <select id="crm-segment-filter" class="form-select">
            <option value="all">Все сегменты</option>
            <option value="customers_all" ${CRM_FILTERS.segment === 'customers_all' ? 'selected' : ''}>Все клиенты</option>
            <option value="leads_active" ${CRM_FILTERS.segment === 'leads_active' ? 'selected' : ''}>Лиды активные</option>
            <option value="leads_archived" ${CRM_FILTERS.segment === 'leads_archived' ? 'selected' : ''}>Лиды архив</option>
            <option value="clients_active" ${CRM_FILTERS.segment === 'clients_active' ? 'selected' : ''}>Клиенты активные</option>
            <option value="clients_archived" ${CRM_FILTERS.segment === 'clients_archived' ? 'selected' : ''}>Клиенты архив</option>
          </select>
        </div>
        <div class="col-md-3 col-sm-12">
          <input id="crm-q-filter" class="form-control" placeholder="Поиск: id, имя, телефон, email, telegram" value="${esc(CRM_FILTERS.q || '')}"/>
        </div>
        <div class="col-md-2 col-sm-6">
          <select id="crm-limit-filter" class="form-select">
            <option value="50" ${Number(CRM_FILTERS.limit || 100) === 50 ? 'selected' : ''}>50</option>
            <option value="100" ${Number(CRM_FILTERS.limit || 100) === 100 ? 'selected' : ''}>100</option>
            <option value="200" ${Number(CRM_FILTERS.limit || 100) === 200 ? 'selected' : ''}>200</option>
          </select>
        </div>
        <div class="col-md-2 col-sm-6 d-flex gap-2">
          <button class="btn" onclick="applyCrmFilters()">Применить</button>
          <button class="btn ghost" onclick="resetCrmFilters()">Сброс</button>
        </div>
      </div>
      <div class="small muted" style="margin-top:10px">
        Пользователи: ${esc(users.count ?? 0)} · Показано: ${esc((users.items || []).length)} · Выбран: ${esc(selectedCustomerText)}
      </div>
      ${users.ready === false && users.error ? `<div class="code" style="margin-top:10px">${esc(users.error)}</div>` : ''}
    </div>

    <div class="table-wrap">
      <table class="table">
        <thead>
          <tr>
            <th>customer_id</th>
            <th>Сегмент</th>
            <th>Имя</th>
            <th>Телефон</th>
            <th>Email</th>
            <th>Telegram</th>
            <th>Учится</th>
            <th>Удалён</th>
            <th>Оплачено до</th>
            <th>Оплат/уроки</th>
            <th>Баланс</th>
            <th>CRM статус</th>
            <th>Дата</th>
            <th>Действие</th>
          </tr>
        </thead>
        <tbody>
          ${(users.items || []).length ? (users.items || []).map((r) => {
            const selected = CRM_SELECTED_CUSTOMER_ID !== null && Number(CRM_SELECTED_CUSTOMER_ID) === Number(r.customer_id);
            const style = selected ? ' style="background:#eef2ff;"' : '';
            return `
            <tr${style}>
              <td><b>${esc(r.customer_id)}</b></td>
              <td>${esc(crmSegmentLabel(r.segment))}</td>
              <td>${esc(r.customer_name || '-')}</td>
              <td>${esc(r.phone_normalized || '-')}</td>
              <td>${esc(r.email_normalized || '-')}</td>
              <td>${esc(r.telegram_username || '-')}</td>
              <td>${esc(crmBoolLabel(r.is_study))}</td>
              <td>${esc(crmBoolLabel(r.removed))}</td>
              <td>${esc(r.paid_till || '-')}</td>
              <td>${esc(`${r.paid_count || '0'} / ${r.paid_lesson_count || '0'}`)}</td>
              <td>${esc(r.balance || '-')}</td>
              <td>${esc(`study:${r.study_status_id || '-'} lead:${r.lead_status_id || '-'}`)}</td>
              <td>${esc(r.report_date || '-')}</td>
              <td><button class="btn" onclick="selectCrmCustomer(${Number(r.customer_id)}, '${escapeJsSingleQuoted(r.customer_name || '')}')">Показать коммуникации</button></td>
            </tr>`;
          }).join('') : `<tr><td colspan="14"><div class="empty">Нет пользователей CRM по выбранным фильтрам</div></td></tr>`}
        </tbody>
      </table>
    </div>

    <div class="panel" style="margin-top:12px">
      <div class="panel-title">Коммуникации клиента</div>
      <div class="small muted">Записей: ${esc(comms.count ?? 0)} · customer_id: ${esc(meta.selected_customer_id ?? '-')}</div>
      ${comms.ready === false && comms.error ? `<div class="code" style="margin-top:10px">${esc(comms.error)}</div>` : ''}
      <div class="table-wrap" style="margin-top:10px">
        <table class="table">
          <thead>
            <tr>
              <th>report_date</th>
              <th>communication_id</th>
              <th>type</th>
              <th>created_at</th>
              <th>source_file</th>
              <th>payload</th>
            </tr>
          </thead>
          <tbody>
            ${(comms.items || []).length ? (comms.items || []).map((r) => `
              <tr>
                <td>${esc(r.report_date || '-')}</td>
                <td>${esc(r.communication_id || r.row_key || '-')}</td>
                <td>${esc(r.communication_type || '-')}</td>
                <td>${esc(r.created_at || '-')}</td>
                <td>${esc(r.source_file || '-')}</td>
                <td><div class="code">${esc(shortText(JSON.stringify(r.payload_json || {}), 220))}</div></td>
              </tr>
            `).join('') : `<tr><td colspan="6"><div class="empty">Нет коммуникаций для выбранного клиента</div></td></tr>`}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

async function loadCrmData(){
  const usersParams = new URLSearchParams();
  usersParams.set('limit', String(Number(CRM_FILTERS.limit || 100)));
  usersParams.set('offset', String(Number(CRM_FILTERS.offset || 0)));
  if ((CRM_FILTERS.report_date || '').trim()) usersParams.set('report_date', (CRM_FILTERS.report_date || '').trim());
  if (CRM_FILTERS.segment && CRM_FILTERS.segment !== 'all') usersParams.set('segment', CRM_FILTERS.segment);
  if ((CRM_FILTERS.q || '').trim()) usersParams.set('q', (CRM_FILTERS.q || '').trim());

  const [statusRes, usersRes] = await Promise.allSettled([
    api('/api/crm/load-status?limit=100'),
    api('/api/crm/users?' + usersParams.toString()),
  ]);

  const status = statusRes.status === 'fulfilled'
    ? statusRes.value
    : { ready: false, items: [], count: 0, error: statusRes.reason?.message || 'ошибка API /api/crm/load-status' };
  const users = usersRes.status === 'fulfilled'
    ? usersRes.value
    : { ready: false, items: [], count: 0, limit: Number(CRM_FILTERS.limit || 100), offset: Number(CRM_FILTERS.offset || 0), error: usersRes.reason?.message || 'ошибка API /api/crm/users' };

  DASH.crm_load_status = status || { ready: false, items: [], count: 0 };
  DASH.crm_users = users || { ready: false, items: [], count: 0 };

  const items = users.items || [];
  const hasSelected = items.some((x) => Number(x.customer_id) === Number(CRM_SELECTED_CUSTOMER_ID));
  if (!hasSelected) {
    CRM_SELECTED_CUSTOMER_ID = items.length ? Number(items[0].customer_id) : null;
  }
  const selectedItem = items.find((x) => Number(x.customer_id) === Number(CRM_SELECTED_CUSTOMER_ID)) || null;
  DASH.crm_meta = {
    ready: true,
    selected_customer_id: CRM_SELECTED_CUSTOMER_ID,
    selected_customer_name: selectedItem?.customer_name || '',
  };

  if (CRM_SELECTED_CUSTOMER_ID === null) {
    DASH.crm_communications = { ready: true, items: [], count: 0, limit: Number(CRM_FILTERS.communications_limit || 100), offset: 0 };
    return;
  }

  const commParams = new URLSearchParams();
  commParams.set('customer_id', String(CRM_SELECTED_CUSTOMER_ID));
  commParams.set('limit', String(Number(CRM_FILTERS.communications_limit || 100)));
  commParams.set('offset', String(Number(CRM_FILTERS.communications_offset || 0)));
  if ((CRM_FILTERS.report_date || '').trim()) commParams.set('report_date', (CRM_FILTERS.report_date || '').trim());

  try {
    const comms = await api('/api/crm/communications?' + commParams.toString());
    DASH.crm_communications = comms || { ready: true, items: [], count: 0 };
  } catch (e) {
    DASH.crm_communications = { ready: false, items: [], count: 0, error: e.message || 'ошибка API /api/crm/communications' };
  }
}

async function loadCrmDataAndRender(){
  try {
    await loadCrmData();
    if (CURRENT_SECTION === 'crm') renderCrm();
  } catch (e) {
    alert('Ошибка загрузки CRM: ' + e.message);
  }
}

async function applyCrmFilters(){
  CRM_FILTERS.report_date = String(document.getElementById('crm-report-date-filter')?.value || '').trim();
  CRM_FILTERS.segment = String(document.getElementById('crm-segment-filter')?.value || 'all').trim() || 'all';
  CRM_FILTERS.q = String(document.getElementById('crm-q-filter')?.value || '').trim();
  CRM_FILTERS.limit = Number(document.getElementById('crm-limit-filter')?.value || 100);
  if (![50, 100, 200].includes(Number(CRM_FILTERS.limit))) {
    CRM_FILTERS.limit = 100;
  }
  CRM_FILTERS.offset = 0;
  await loadCrmDataAndRender();
}

async function resetCrmFilters(){
  CRM_FILTERS = { report_date: '', segment: 'all', q: '', limit: 100, offset: 0, communications_limit: 100, communications_offset: 0 };
  CRM_SELECTED_CUSTOMER_ID = null;
  await loadCrmDataAndRender();
}

async function selectCrmCustomer(customerId, customerName=''){
  const n = Number(customerId);
  if (!Number.isFinite(n) || n <= 0) return;
  CRM_SELECTED_CUSTOMER_ID = n;
  DASH.crm_meta = {
    ...(DASH.crm_meta || {}),
    selected_customer_id: n,
    selected_customer_name: String(customerName || ''),
  };
  await loadCrmDataAndRender();
}

function applyCrmSyncOptionsFromForm(){
  CRM_SYNC_OPTIONS.updates_only = !!document.getElementById('crm-sync-updates-only')?.checked;
  CRM_SYNC_OPTIONS.include_communications = !!document.getElementById('crm-sync-include-comms')?.checked;
  CRM_SYNC_OPTIONS.include_lessons = !!document.getElementById('crm-sync-include-lessons')?.checked;
  CRM_SYNC_OPTIONS.include_extra = !!document.getElementById('crm-sync-include-extra')?.checked;
  const timeoutValue = Number(document.getElementById('crm-sync-timeout-sec')?.value || CRM_SYNC_OPTIONS.timeout_sec || 1800);
  CRM_SYNC_OPTIONS.timeout_sec = Math.min(7200, Math.max(60, Number.isFinite(timeoutValue) ? timeoutValue : 1800));
}

async function runCrmDirectSync(){
  applyCrmSyncOptionsFromForm();
  const reportDate = crmPromptDate(CRM_FILTERS.report_date || new Date().toISOString().slice(0, 10));
  if (reportDate === null) return;
  try {
    const res = await api('/api/crm/direct-sync', {
      method: 'POST',
      timeoutMs: Number(CRM_SYNC_OPTIONS.timeout_sec || 1800) * 1000 + 30000,
      body: JSON.stringify({
        report_date: reportDate || undefined,
        updates_only: !!CRM_SYNC_OPTIONS.updates_only,
        include_communications: !!CRM_SYNC_OPTIONS.include_communications,
        include_lessons: !!CRM_SYNC_OPTIONS.include_lessons,
        include_extra: !!CRM_SYNC_OPTIONS.include_extra,
        timeout_sec: Number(CRM_SYNC_OPTIONS.timeout_sec || 1800),
      }),
    });
    const load = res.load_result || {};
    alert(
      `SERM синхронизация завершена.\n` +
      `source: ${res.source || '-'}\n` +
      `source_file: ${load.source_file || '-'}\n` +
      `users_rows: ${load.customers_rows ?? 0}\n` +
      `communications_rows: ${load.communications_rows ?? 0}`
    );
    if (reportDate) CRM_FILTERS.report_date = reportDate;
    await loadCrmDataAndRender();
  } catch (e) {
    alert('Ошибка синхронизации SERM: ' + e.message);
  }
}

async function runCrmLoadFile(){
  const xlsxPath = prompt('Укажите путь к XLSX-файлу AlfaCRM', '');
  if (xlsxPath === null) return;
  const xlsx = String(xlsxPath || '').trim();
  if (!xlsx) {
    alert('Путь к файлу обязателен');
    return;
  }

  const reportDate = crmPromptDate(CRM_FILTERS.report_date || new Date().toISOString().slice(0, 10));
  if (reportDate === null) return;
  const skipComms = confirm('Пропустить загрузку листа communications?\nOK = пропустить, Cancel = загружать communications.');

  try {
    const res = await api('/api/crm/load-file', {
      method: 'POST',
      timeoutMs: 1800000,
      body: JSON.stringify({
        xlsx_path: xlsx,
        report_date: reportDate || undefined,
        skip_communications: skipComms,
      }),
    });
    alert(
      `CRM загрузка завершена.\n` +
      `source_file: ${res.source_file || '-'}\n` +
      `users_rows: ${res.customers_rows ?? 0}\n` +
      `communications_rows: ${res.communications_rows ?? 0}`
    );
    if (reportDate) CRM_FILTERS.report_date = reportDate;
    await loadCrmDataAndRender();
  } catch (e) {
    alert('Ошибка загрузки CRM: ' + e.message);
  }
}

function auditSourceBadge(source, cacheHit){
  const s = String(source || '').toLowerCase();
  if (cacheHit || s === 'cache') return '<span class="badge">кэш</span>';
  if (s === 'openrouter') return '<span class="badge good">OpenRouter</span>';
  return `<span class="badge">${esc(s || '-')}</span>`;
}

function auditStatusText(status){
  const s = String(status || '').toLowerCase();
  if (s === 'pending') return 'Ожидает';
  if (s === 'running') return 'Выполняется';
  if (s === 'reviewed') return 'Проверен';
  if (s === 'requires_fix') return 'Нужна доработка';
  if (s === 'approved') return 'Одобрен';
  if (s === 'failed') return 'Ошибка';
  return status || '-';
}

function auditReviewText(status){
  const s = String(status || '').toLowerCase();
  if (s === 'approved') return 'Принято';
  if (s === 'rejected') return 'Отклонено';
  if (s === 'needs_fix') return 'Нужна доработка';
  return '-';
}

function renderAuditRunDetail(run){
  if (!run) {
    return '<div class="empty">Выберите run для деталей</div>';
  }
  const normalized = run.response_json || {};
  const raw = run.raw_response_json || {};
  const notification = run.notification || {};
  const runId = Number(run.id || 0);
  return `
    <div class="panel">
      <div class="panel-title">Run #${esc(run.id)} ${helpDot('Карточка одного запуска аудита: итог, статус, ошибки и технический след.')}</div>
      <div class="small muted">Создан: ${esc(run.created_at || '-')} · Обновлён: ${esc(run.updated_at || '-')}</div>
      <div class="row mt-2">
        <span class="badge">Статус: ${esc(auditStatusText(run.status))}</span>
        ${auditSourceBadge(run.source, run.cache_hit)}
        <span class="badge">Кэш: ${run.cache_hit ? 'да' : 'нет'}</span>
        <span class="badge">Попыток: ${esc(run.attempt_count ?? 0)}</span>
        <span class="badge">HTTP: ${esc(run.transport_status_code ?? '-')}</span>
        <span class="badge">External: ${esc(run.external_status || '-')}</span>
        <span class="badge">Повторяемая: ${run.retryable ? 'да' : 'нет'}</span>
        <span class="badge">Проверка: ${esc(auditReviewText(run.review_status))}</span>
      </div>
      ${runId > 0 ? `<div class="small muted mt-2">Страница: <a href="/audits/${runId}" target="_blank">/audits/${runId}</a></div>` : ''}
      <div class="small muted mt-2">cache_key: ${esc(run.cache_key || '-')}</div>
      <div class="small muted mt-1">Тип ошибки: ${esc(run.last_error_type || '-')}</div>
      <div class="code mt-2">${esc(shortText(run.last_error_text || run.last_error || '-', 800))}</div>
      ${notification.summary ? `<div class="small muted mt-2">Кратко: ${esc(notification.summary)}</div>` : ''}
      <div class="row mt-2">
        <button class="btn primary" onclick="reviewAuditRun(${runId}, 'approve')">Принять</button>
        <button class="btn" onclick="reviewAuditRun(${runId}, 'needs_fix')">Нужна доработка</button>
        <button class="btn" onclick="reviewAuditRun(${runId}, 'reject')">Отклонить</button>
      </div>
      <details class="mt-2">
        <summary><b>Технические детали ${helpDot('Нормализованный ответ = приведённая структура для UI. Raw = исходный ответ модели от провайдера.')}</b></summary>
        <div class="row mt-2">
          <div class="col-md-6 col-sm-12">
            <div class="panel-title" style="font-size:14px">Нормализованный ответ (preview)</div>
            <div class="code">${esc(shortText(JSON.stringify(normalized, null, 2), 2500))}</div>
          </div>
          <div class="col-md-6 col-sm-12">
            <div class="panel-title" style="font-size:14px">Raw ответ (preview)</div>
            <div class="code">${esc(shortText(JSON.stringify(raw, null, 2), 2500))}</div>
          </div>
        </div>
      </details>
    </div>
  `;
}

async function reviewAuditRun(runId, decision){
  if (!runId || runId <= 0) {
    alert('Сначала выберите run');
    return;
  }
  const comment = window.prompt('Комментарий (опционально):', '') || '';
  try {
    await api('/api/audits/runs/' + encodeURIComponent(runId) + '/review', {
      method: 'POST',
      body: JSON.stringify({ decision, comment }),
    });
    await loadAuditsDataAndRender();
    await viewAuditRunDetail(runId);
  } catch (e) {
    alert('Ошибка подтверждения: ' + e.message);
  }
}

function renderAudits(){
  const health = DASH.audits_health || {};
  const runsWrap = DASH.audits_runs || {};
  const rows = runsWrap.items || [];
  const selectedId = Number(DASH.audits_meta?.selected_run_id || 0);
  const selected = rows.find((x) => Number(x.id) === selectedId) || DASH.audits_meta?.selected_run || null;
  const runResult = DASH.audits_meta?.run_result || null;

  document.getElementById('section-audits').innerHTML = `
    <div class="panel">
      <div class="row" style="justify-content:space-between">
        <div class="panel-title" style="margin-bottom:0">Аудит OpenRouter ${helpDot('Внешняя архитектурная проверка изменений. Здесь видно здоровье канала, запуски и их результат.')}</div>
        <div class="row">
          <button class="btn" onclick="loadAuditsDataAndRender()">Обновить данные</button>
          <button class="btn primary" onclick="runAuditSmokeRun()">Тестовый запуск аудита</button>
        </div>
      </div>
      <div class="small muted" style="margin-top:8px">
        Канал: ${health.ok ? '<span class="badge good">доступен</span>' : '<span class="badge bad">ошибка</span>'}
        · checked_at ${esc(health.checked_at || '-')}
        · latency ${esc(health.latency_ms ?? '-')} ms
        · error_class ${esc(health.error_class || '-')}
      </div>
      <div class="small muted" style="margin-top:6px">
        ${helpDot('Если канал недоступен, бизнес-часть системы не блокируется, но run помечается warning/failed и требует ручной разбор.')}${helpDot('Кэш используется только при совпадении входного контекста, версии промпта и ветки.')}
      </div>
      ${health.error ? `<div class="code" style="margin-top:8px">${esc(health.error)}</div>` : ''}
      ${runResult ? `<div class="code" style="margin-top:8px">${esc(shortText(JSON.stringify(runResult), 1200))}</div>` : ''}
    </div>

    <div class="panel">
      <div class="panel-title">Последние запуски аудита ${helpDot('Список run-ов: сверху новые. Выберите строку, чтобы открыть детальную карточку ниже.')}</div>
      <div class="small muted">Всего: ${esc(runsWrap.count ?? rows.length)} · Показано: ${esc(rows.length)}</div>
      ${runsWrap.error ? `<div class="code" style="margin-top:8px">${esc(runsWrap.error)}</div>` : ''}
      <div class="table-wrap" style="margin-top:10px">
        <table class="table">
          <thead>
            <tr>
              <th>created_at ${helpDot('Время создания запуска аудита.')}</th>
              <th>ID ${helpDot('Уникальный номер audit run.')}</th>
              <th>Статус ${helpDot('Текущее состояние выполнения: pending/running/requires_fix/approved/failed.')}</th>
              <th>Источник ${helpDot('openrouter = живой вызов, cache = результат взят из audit_cache.')}</th>
              <th>Кэш ${helpDot('Показывает, был ли использован сохранённый ответ без нового внешнего вызова.')}</th>
              <th>Попытки ${helpDot('Количество реальных попыток вызова внешнего провайдера.')}</th>
              <th>Повторяемая ${helpDot('Можно ли автоматически повторить после ошибки.')}</th>
              <th>Ошибка ${helpDot('Короткое последнее сообщение об ошибке (если было).')}</th>
              <th>Детали</th>
            </tr>
          </thead>
          <tbody>
            ${rows.length ? rows.map((r) => {
              const isSelected = Number(r.id) === selectedId;
              return `
                <tr${isSelected ? ' style="background:#eef2ff;"' : ''}>
                  <td>${esc(r.created_at || '-')}</td>
                  <td><b>${esc(r.id)}</b></td>
                  <td>${statusBadge(auditStatusText(r.status))}</td>
                  <td>${auditSourceBadge(r.source, r.cache_hit)}</td>
                  <td>${r.cache_hit ? 'да' : 'нет'}</td>
                  <td>${esc(r.attempt_count ?? 0)}</td>
                  <td>${r.retryable ? 'да' : 'нет'}</td>
                  <td><div class="code">${esc(shortText(r.last_error || '-', 120))}</div></td>
                  <td><button class="btn" onclick="viewAuditRunDetail(${Number(r.id)})">Открыть</button></td>
                </tr>
              `;
            }).join('') : `<tr><td colspan="9"><div class="empty">Пока нет audit runs</div></td></tr>`}
          </tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <div class="panel-title">Детали запуска ${helpDot('Подробный разбор выбранного run: результат, причины, тех. след и кнопки подтверждения.')}</div>
      ${renderAuditRunDetail(selected)}
    </div>
  `;
}

async function loadAuditsData(){
  const [healthRes, runsRes] = await Promise.allSettled([
    api('/api/audits/health/openrouter?timeout_sec=20'),
    api('/api/audits/runs?limit=50'),
  ]);

  DASH.audits_health = healthRes.status === 'fulfilled'
    ? (healthRes.value || { ok: false })
    : { ok: false, error_class: 'request', error: healthRes.reason?.message || 'health request failed' };

  DASH.audits_runs = runsRes.status === 'fulfilled'
    ? (runsRes.value || { items: [], count: 0 })
    : { items: [], count: 0, error: runsRes.reason?.message || 'runs request failed' };
}

async function loadAuditsDataAndRender(){
  try {
    await loadAuditsData();
    if (CURRENT_SECTION === 'audits') {
      renderAudits();
    }
  } catch (e) {
    alert('Ошибка загрузки аудитов: ' + e.message);
  }
}

async function viewAuditRunDetail(runId){
  try {
    const data = await api('/api/audits/runs/' + encodeURIComponent(runId));
    DASH.audits_meta = {
      ...(DASH.audits_meta || {}),
      selected_run_id: Number(runId),
      selected_run: { ...(data.item || {}), notification: data.notification || null },
    };
    if (CURRENT_SECTION === 'audits') renderAudits();
  } catch (e) {
    alert('Ошибка загрузки деталей run: ' + e.message);
  }
}

async function runAuditSmokeRun(){
  const stage = `ui_smoke_${Date.now()}`;
  const branch = `ui/${window.location.hostname || 'local'}`;
  DASH.audits_meta = { ...(DASH.audits_meta || {}), run_result: null, loading: true };
  if (CURRENT_SECTION === 'audits') renderAudits();

  try {
    const created = await api('/api/audits/runs/create', {
      method: 'POST',
      body: JSON.stringify({
        project_id: 'traffic-analytics',
        branch,
        stage,
        audit_level: 'mini',
        changed_modules_json: ['webapp/app.py', 'webapp/static/admin.js'],
      }),
    });
    const createdId = Number(created?.item?.id || 0);
    const worker = await api('/api/audits/worker/openrouter/run', {
      method: 'POST',
      timeoutMs: 120000,
      body: JSON.stringify({ limit: 10, timeout_sec: 60, max_retries: 2 }),
    });

    let detail = null;
    if (createdId > 0) {
      const detailRes = await api('/api/audits/runs/' + createdId);
      detail = { ...(detailRes.item || {}), notification: detailRes.notification || null };
    }

    DASH.audits_meta = {
      ...(DASH.audits_meta || {}),
      selected_run_id: createdId || null,
      selected_run: detail,
      run_result: { created_id: createdId || null, worker },
      loading: false,
    };
    await loadAuditsDataAndRender();
  } catch (e) {
    DASH.audits_meta = {
      ...(DASH.audits_meta || {}),
      run_result: { error: e.message || 'smoke run failed' },
      loading: false,
    };
    if (CURRENT_SECTION === 'audits') renderAudits();
    alert('Ошибка тестового запуска аудита: ' + e.message);
  }
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
  if (!IS_SCORING_STANDALONE && !IS_AUDITS_STANDALONE) {
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
  if (CURRENT_SECTION === 'crm') renderCrm();
  if (CURRENT_SECTION === 'audits') renderAudits();
  if (CURRENT_SECTION === 'actions') renderActions();
  if (CURRENT_SECTION === 'diagnostics') renderDiagnostics();
  decorateNavIcons();
  decorateButtonIcons();
  renderIconLibraries();
  decorateUiHelpHints();
  renderIconLibraries();
  window.requestAnimationFrame(() => decorateUiHelpHints());
  window.requestAnimationFrame(() => {
    decorateNavIcons();
    decorateButtonIcons();
    renderIconLibraries();
  });
}

async function reloadAll(){
  try{
    const dashboard = await api('/api/full-dashboard');
    DASH = {
      ...DASH,
      ...dashboard,
    };
    await loadScoringData();
    if (CURRENT_SECTION === 'crm') {
      await loadCrmData();
    }
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
  applyStandaloneAuditsLayout();
  decorateNavIcons();
  decorateButtonIcons();
  renderIconLibraries();
  loadSystemVersionMeta();
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
  } else if (IS_AUDITS_STANDALONE) {
    await loadAuditsData();
    renderSection();
  } else {
    await reloadAll();
  }
});
