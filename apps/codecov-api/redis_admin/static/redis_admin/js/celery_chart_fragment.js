// Lazy-fetches the celery_broker frequency chart fragment for the
// current drill-down queue. Triggered by the placeholder div
// injected by templates/admin/redis_admin/celerybrokerqueue/change_list.html.
//
// Loaded as an external file (not inline) so the strict CSP on
// production admin (api-admin.codecov.io) — which only allows
// `'self'` and one fixed sha256 hash for scripts — does not block
// it. See settings_base.py CSP_DEFAULT_SRC.
(function () {
  function loadChart() {
    var el = document.getElementById('celery-chart-fragment');
    if (!el) return;
    var url = el.dataset.fragmentUrl;
    if (!url) return;
    fetch(url, { credentials: 'same-origin' })
      .then(function (r) {
        if (r.status === 204) {
          el.innerHTML = '';
          return;
        }
        if (!r.ok) {
          el.innerHTML = '<p class="errornote">Could not load frequency chart (HTTP ' + r.status + ').</p>';
          return;
        }
        return r.text().then(function (html) {
          el.innerHTML = html;
        });
      })
      .catch(function () {
        el.innerHTML = '<p class="errornote">Could not load frequency chart.</p>';
      });
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', loadChart);
  } else {
    loadChart();
  }
})();
