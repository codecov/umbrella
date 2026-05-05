// Polls the chunked unacked clear job's status JSON endpoint,
// updates the progress bar + counters, and hijacks the cancel form
// to fire via fetch(). Same shape as celery_clear_progress.js (and
// reuses the celery_clear_progress.css stylesheet) but reads the
// HASH-native counter `zrem_removed` instead of the celery-specific
// `drifted`. Loaded as an external file (not inline) so the
// production CSP — `'self'` + a single fixed sha256 hash for inline
// scripts — does not block it. See settings_base.py CSP_DEFAULT_SRC.
//
// The element IDs reuse the `celery-clear-*` prefix so we can share
// the celery_clear_progress.css stylesheet without duplicating its
// theme-aware rules; the JS is forked rather than parameterised
// because the field rename was not worth a runtime conditional.
(function () {
  'use strict';

  var TERMINAL_STATES = { completed: 1, cancelled: 1, failed: 1 };
  var POLL_INTERVAL_OK_MS = 1000;
  var POLL_BACKOFF_STEPS_MS = [1000, 2000, 5000];
  var POLL_BACKOFF_MAX_MS = 5000;
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
    setText('celery-clear-zrem', snapshot.zrem_removed);
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
