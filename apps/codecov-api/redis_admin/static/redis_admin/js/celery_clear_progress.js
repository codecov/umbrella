// Polls the chunked celery_broker clear job's status JSON endpoint,
// updates the progress bar + counters, and hijacks the cancel form
// to fire via fetch(). Loaded as an external file (not inline) so
// the production CSP on api-admin.codecov.io — `'self'` plus a
// single fixed sha256 hash for inline scripts — does not block it.
// See settings_base.py CSP_DEFAULT_SRC.
//
// Lifecycle:
//   - On DOMContentLoaded, read the job id + URLs from data
//     attributes on `#celery-clear-progress-root`.
//   - If the initial server-rendered status is non-terminal, start
//     polling every 1s. Backs off (1s → 2s → 5s, capped at 5s) on
//     fetch errors so a transient cache outage doesn't hammer the
//     api with retries.
//   - On terminal status (`completed` / `cancelled` / `failed`),
//     stop polling, disable the Cancel button, and enable the
//     "back to changelist" link.
//   - The Cancel button click fires a fetch() POST to the cancel
//     URL with the CSRF token. The button reads "Cancelling…" until
//     the next poll confirms `status=cancelled`. Double-clicks are
//     prevented by toggling `disabled` while a request is in flight.
(function () {
  'use strict';

  var TERMINAL_STATES = { completed: 1, cancelled: 1, failed: 1 };
  var POLL_INTERVAL_OK_MS = 1000;
  var POLL_BACKOFF_STEPS_MS = [1000, 2000, 5000];
  var POLL_BACKOFF_MAX_MS = 5000;
  // Enumerated rather than stripped by prefix because the pill's
  // base class (`celery-clear-status-pill`) shares the same prefix
  // as the state-specific classes; a prefix-strip would drop the
  // base styling (display, padding, border-radius, etc.) on the
  // first poll update and leave only the colour overrides.
  var STATE_CLASSES = [
    'celery-clear-status-pending',
    'celery-clear-status-running',
    'celery-clear-status-completed',
    'celery-clear-status-cancelled',
    'celery-clear-status-failed'
  ];

  function $(id) { return document.getElementById(id); }

  function getCsrfToken(form) {
    if (!form) return '';
    var input = form.querySelector('input[name=csrfmiddlewaretoken]');
    return input ? input.value : '';
  }

  function setText(id, value) {
    var el = $(id);
    if (el) el.textContent = value;
  }

  function setStatusPill(status) {
    var pill = $('celery-clear-status-pill');
    if (!pill) return;
    pill.textContent = status;
    pill.dataset.status = status;
    // Strip any prior state class then add the current one. We
    // explicitly enumerate STATE_CLASSES rather than filtering by
    // prefix so the base `celery-clear-status-pill` class survives
    // and the pill keeps its layout / typography styling.
    for (var i = 0; i < STATE_CLASSES.length; i += 1) {
      pill.classList.remove(STATE_CLASSES[i]);
    }
    pill.classList.add('celery-clear-status-' + status);
  }

  function updateProgress(snapshot) {
    var bar = $('celery-clear-progress-bar');
    var pct = $('celery-clear-progress-pct');
    var total = snapshot.total_estimated || 0;
    var processed = snapshot.processed || 0;
    if (bar) {
      bar.value = processed;
      // <progress> with max=0 renders as indeterminate; use 1 as a
      // floor so the bar shows zero progress rather than spinning.
      bar.max = total > 0 ? total : 1;
    }
    if (pct) {
      if (total > 0) {
        var p = Math.min(100, Math.floor((processed / total) * 100));
        pct.textContent = p + '%';
      } else {
        pct.textContent = '—';
      }
    }
    setText('celery-clear-matched', snapshot.matched);
    setText('celery-clear-processed', snapshot.processed);
    setText('celery-clear-total-estimated', snapshot.total_estimated);
    setText('celery-clear-drifted', snapshot.drifted);
    setText('celery-clear-passes', snapshot.passes_run);
    setText('celery-clear-started-at', snapshot.started_at || '—');
    setText('celery-clear-updated-at', snapshot.updated_at || '—');
    setText('celery-clear-completed-at', snapshot.completed_at || '—');
    var errorEl = $('celery-clear-error');
    var errorRow = $('celery-clear-error-row');
    if (errorEl && errorRow) {
      if (snapshot.error) {
        errorEl.textContent = snapshot.error;
        errorRow.classList.remove('celery-clear-hidden');
      } else {
        errorEl.textContent = '';
        errorRow.classList.add('celery-clear-hidden');
      }
    }
  }

  function isTerminal(status) {
    return Object.prototype.hasOwnProperty.call(TERMINAL_STATES, status);
  }

  function setTerminalUI(status) {
    var cancelBtn = $('celery-clear-cancel-button');
    if (cancelBtn) {
      cancelBtn.disabled = true;
      // Show what the final state was, not the in-flight verb.
      cancelBtn.textContent = (
        status === 'cancelled' ? 'Cancelled'
          : status === 'failed' ? 'Failed'
            : 'Done'
      );
    }
    var back = $('celery-clear-back-link');
    if (back) back.classList.remove('celery-clear-disabled-link');
  }

  function init() {
    var root = $('celery-clear-progress-root');
    if (!root) return;
    var statusUrl = root.dataset.statusUrl;
    var cancelUrl = root.dataset.cancelUrl;
    if (!statusUrl) return;

    var initialStatus = root.dataset.initialStatus || 'pending';
    var initialTerminal = root.dataset.isTerminal === '1';
    if (initialTerminal) {
      setTerminalUI(initialStatus);
      return;
    }

    var backoffStep = 0;
    var pollTimer = null;
    var inFlight = false;

    function scheduleNextPoll(intervalMs) {
      if (pollTimer) clearTimeout(pollTimer);
      pollTimer = setTimeout(poll, intervalMs);
    }

    function poll() {
      if (inFlight) return;
      inFlight = true;
      fetch(statusUrl, { credentials: 'same-origin', headers: { 'Accept': 'application/json' } })
        .then(function (r) {
          if (!r.ok) {
            throw new Error('status HTTP ' + r.status);
          }
          return r.json();
        })
        .then(function (snapshot) {
          backoffStep = 0;
          updateProgress(snapshot);
          var status = snapshot.status || 'pending';
          setStatusPill(status);
          var cancelBtn = $('celery-clear-cancel-button');
          if (cancelBtn && snapshot.cancel_requested && !isTerminal(status)) {
            cancelBtn.textContent = 'Cancelling…';
            cancelBtn.disabled = true;
          }
          if (isTerminal(status)) {
            setTerminalUI(status);
            return;
          }
          scheduleNextPoll(POLL_INTERVAL_OK_MS);
        })
        .catch(function () {
          // Backoff on errors. Step through 1s → 2s → 5s, then stay
          // at 5s. Doesn't escalate to "give up": a transient cache
          // outage may resolve and we still want the page to update
          // when it does.
          var step = backoffStep < POLL_BACKOFF_STEPS_MS.length
            ? POLL_BACKOFF_STEPS_MS[backoffStep]
            : POLL_BACKOFF_MAX_MS;
          backoffStep += 1;
          scheduleNextPoll(step);
        })
        .then(function () { inFlight = false; });
    }

    var cancelForm = $('celery-clear-cancel-form');
    if (cancelForm && cancelUrl) {
      cancelForm.addEventListener('submit', function (e) {
        e.preventDefault();
        var cancelBtn = $('celery-clear-cancel-button');
        if (cancelBtn) {
          if (cancelBtn.disabled) return;
          cancelBtn.disabled = true;
          cancelBtn.textContent = 'Cancelling…';
        }
        fetch(cancelUrl, {
          method: 'POST',
          credentials: 'same-origin',
          headers: {
            'X-CSRFToken': getCsrfToken(cancelForm),
            'Accept': 'application/json'
          }
        })
          .then(function () { /* poll will pick up the new state */ })
          .catch(function () {
            // If the cancel POST itself errored we re-enable the
            // button so the operator can retry; the next poll will
            // also reflect whatever the server thinks the state is.
            if (cancelBtn) {
              cancelBtn.disabled = false;
              cancelBtn.textContent = 'Cancel clear';
            }
          });
      });
    }

    poll();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
