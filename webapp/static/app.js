async function openDiagnosticModal() {
  const modal = document.getElementById('diag-modal');
  const output = document.getElementById('diag-output');
  const meta = document.getElementById('diag-meta');

  if (!modal || !output || !meta) {
    alert('Диагностические элементы интерфейса не найдены');
    console.error('Missing diagnostic elements', { modal, output, meta });
    return;
  }

  modal.style.display = 'block';
  output.value = 'Собираю диагностику...';
  meta.textContent = 'running...';

  try {
    const res = await fetch('/api/diagnostic', {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' }
    });

    const text = await res.text();
    let data;
    try {
      data = JSON.parse(text);
    } catch (e) {
      output.value = 'Некорректный ответ API:\\n\\n' + text;
      meta.textContent = 'invalid json';
      console.error('Diagnostic invalid JSON', text);
      return;
    }

    output.value = data.content || '';
    meta.textContent = `ok=${data.ok} returncode=${data.returncode} report=${data.report_path}`;
  } catch (e) {
    output.value = 'Ошибка запуска диагностики: ' + e.message;
    meta.textContent = 'failed';
    console.error('Diagnostic fetch failed', e);
    alert('Ошибка запуска диагностики: ' + e.message);
  }
}

function closeDiagnosticModal() {
  const modal = document.getElementById('diag-modal');
  if (modal) modal.style.display = 'none';
}

function copyDiagnosticReport() {
  const output = document.getElementById('diag-output');
  if (!output) {
    alert('Поле отчёта не найдено');
    return;
  }
  navigator.clipboard.writeText(output.value || '');
  alert('Диагностический отчёт скопирован');
}

window.openDiagnosticModal = openDiagnosticModal;
window.closeDiagnosticModal = closeDiagnosticModal;
window.copyDiagnosticReport = copyDiagnosticReport;

document.addEventListener('click', function (e) {
  const btn = e.target.closest('#diag-btn');
  if (btn) {
    e.preventDefault();
    openDiagnosticModal();
    return;
  }

  const closeBtn = e.target.closest('#diag-close-btn');
  if (closeBtn) {
    e.preventDefault();
    closeDiagnosticModal();
    return;
  }

  const copyBtn = e.target.closest('#diag-copy-btn');
  if (copyBtn) {
    e.preventDefault();
    copyDiagnosticReport();
    return;
  }
});

document.addEventListener('DOMContentLoaded', function () {
  console.log('diagnostic app.js loaded');
});
