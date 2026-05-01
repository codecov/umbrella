// Lazy-loads the count + sample table for the clear-by-filter
// preview page. The Django GET handler renders only the form +
// skeleton placeholders so the page paints in <200ms even on
// million-message queues; this script then fetches the JSON
// preview endpoint and either:
//
//   * fills in the count + sample synchronously (small queues), or
//   * polls the chunked-clear-job status endpoint (deep queues
//     above REDIS_ADMIN_CLEAR_BY_FILTER_PREVIEW_INLINE_LIMIT),
//     reading `total_found` off the job hash on completion as the
//     match count.
//
// Loaded as an external file (not inline) so the production CSP on
// api-admin.codecov.io — `'self'` plus a single fixed sha256 hash
// for inline scripts — does not block it. See settings_base.py
// CSP_DEFAULT_SRC.
//
// Lifecycle:
//   - On DOMContentLoaded, read the preview URL + skip-count URL +
//     filter params from data attributes on
//     `#celery-clear-by-filter-preview-root`.
//   - If `data-count-skipped="1"` is set, leave the form alone (the
//     server already rendered with the count card hidden + submit
//     buttons unlocked) and exit.
//   - Otherwise fetch the preview endpoint with same-origin creds.
//   - mode=synchronous → fill count, render sample, unlock submits.
//   - mode=job → switch to polling the job status endpoint; same
//     backoff shape as celery_clear_progress.js. On completed, read
//     `matched` (or `total_found`) as the count and unlock submits.
//   - On error → show a non-fatal error pill with the skip-count
//     escape link.
(function () {
  'use strict';

  var POLL_INTERVAL_OK_MS = 1000;
  var POLL_BACKOFF_STEPS_MS = [1000, 2000, 5000];
  var POLL_BACKOFF_MAX_MS = 5000;
  var TERMINAL_STATES = { completed: 1, cancelled: 1, failed: 1 };

  function $(id) { return document.getElementById(id); }

  function setText(id, value) {
    var el = $(id);
    if (el) el.textContent = value;
  }

  function show(el) {
    if (el) el.classList.remove('celery-clear-preview-hidden');
  }

  function hide(el) {
    if (el) el.classList.add('celery-clear-preview-hidden');
  }

  function escapeHtml(s) {
    if (s === null || typeof s === 'undefined') return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatNumber(n) {
    if (typeof n !== 'number' || !isFinite(n)) return '0';
    // Insert thin spaces / commas so a million-deep queue's count
    // is readable. Falls back to a plain toString() in browsers
    // missing Intl support.
    if (typeof Intl !== 'undefined' && Intl.NumberFormat) {
      return new Intl.NumberFormat().format(n);
    }
    return String(n);
  }

  function setSubmitButtonsEnabled(enabled) {
    var buttons = document.querySelectorAll(
      '.celery-clear-actions button[type="submit"]'
    );
    for (var i = 0; i < buttons.length; i += 1) {
      buttons[i].disabled = !enabled;
    }
  }

  function renderSampleTable(rootEl, sample) {
    var tableContainer = $('celery-clear-preview-sample');
    if (!tableContainer) return;
    if (!sample || sample.length === 0) {
      tableContainer.innerHTML = '<p><em>No matching messages in the sample window.</em></p>';
      return;
    }
    var rows = [];
    for (var i = 0; i < sample.length; i += 1) {
      var row = sample[i];
      var commitShort = row.commitid ? String(row.commitid).slice(0, 7) : '';
      var taskShort = row.task_id ? String(row.task_id).slice(0, 8) : '';
      rows.push(
        '<tr>' +
        '<td>' + escapeHtml(row.index_in_queue) + '</td>' +
        '<td>' + escapeHtml(row.task_name || '—') + '</td>' +
        '<td>' + (row.repoid === null || typeof row.repoid === 'undefined'
          ? '—' : escapeHtml(row.repoid)) + '</td>' +
        '<td>' + (commitShort
          ? '<code title="' + escapeHtml(row.commitid) + '">' + escapeHtml(commitShort) + '</code>'
          : '—') + '</td>' +
        '<td>' + (taskShort
          ? '<code title="' + escapeHtml(row.task_id) + '">' + escapeHtml(taskShort) + '</code>'
          : '—') + '</td>' +
        '</tr>'
      );
    }
    tableContainer.innerHTML = (
      '<table>' +
      '<thead><tr>' +
      '<th>Index</th><th>Task</th><th>repoid</th><th>commit</th><th>task id</th>' +
      '</tr></thead>' +
      '<tbody>' + rows.join('') + '</tbody>' +
      '</table>'
    );
  }

  function fillSynchronousResult(rootEl, payload) {
    setText('celery-clear-preview-count', formatNumber(payload.match_count || 0));
    var keptIndex = payload.kept_index;
    var keptIndexEl = $('celery-clear-preview-kept-index-row');
    if (keptIndexEl) {
      if (typeof keptIndex === 'number' && payload.match_count >= 2) {
        setText('celery-clear-preview-kept-index', String(keptIndex));
        setText(
          'celery-clear-preview-keep-one-remaining',
          formatNumber((payload.match_count || 0) - 1)
        );
        setText(
          'celery-clear-preview-keep-one-total',
          formatNumber(payload.match_count || 0)
        );
        show(keptIndexEl);
      } else {
        hide(keptIndexEl);
      }
    }

    // Update the per-button copy with the live count so the
    // operator sees "Clear all (12,345 from notify)" instead of
    // the placeholder. The keep-one button is hidden when count <=
    // 1 because the action is a no-op in that case.
    var keepOneBtn = $('celery-clear-preview-button-keep-one');
    if (keepOneBtn) {
      if ((payload.match_count || 0) >= 2) {
        keepOneBtn.textContent = (
          'Clear all but first (' +
          formatNumber((payload.match_count || 0) - 1) + ' of ' +
          formatNumber(payload.match_count || 0) + ' from ' +
          (rootEl.dataset.queueName || '') + ')'
        );
        show(keepOneBtn);
      } else {
        hide(keepOneBtn);
      }
    }
    var clearAllBtn = $('celery-clear-preview-button-clear-all');
    if (clearAllBtn) {
      clearAllBtn.textContent = (
        'Clear all (' +
        formatNumber(payload.match_count || 0) + ' from ' +
        (rootEl.dataset.queueName || '') + ')'
      );
    }

    // Hide the loader card, reveal the resolved count card, and
    // render the sample. Empty match_count → don't unlock submits;
    // the operator's only legitimate next action is to widen the
    // filter or back out.
    hide($('celery-clear-preview-loader'));
    show($('celery-clear-preview-count-card'));

    if (payload.cached_at) {
      var cachedNote = $('celery-clear-preview-cached-note');
      if (cachedNote) {
        cachedNote.textContent = 'Cached at ' + payload.cached_at;
        show(cachedNote);
      }
    }

    if ((payload.match_count || 0) === 0) {
      hide($('celery-clear-preview-form-area'));
      show($('celery-clear-preview-empty'));
      return;
    }

    show($('celery-clear-preview-form-area'));
    renderSampleTable(rootEl, payload.sample || []);
    setSubmitButtonsEnabled(true);
  }

  function showError(message) {
    hide($('celery-clear-preview-loader'));
    var errBox = $('celery-clear-preview-error');
    if (errBox) {
      errBox.textContent = message;
      show(errBox);
    }
    show($('celery-clear-preview-error-card'));
  }

  function pollJob(rootEl, statusUrl) {
    var backoffStep = 0;
    var pollTimer = null;
    var inFlight = false;

    function scheduleNext(intervalMs) {
      if (pollTimer) clearTimeout(pollTimer);
      pollTimer = setTimeout(poll, intervalMs);
    }

    function poll() {
      if (inFlight) return;
      inFlight = true;
      fetch(statusUrl, {
        credentials: 'same-origin',
        headers: { 'Accept': 'application/json' }
      })
        .then(function (r) {
          if (!r.ok) {
            throw new Error('status HTTP ' + r.status);
          }
          return r.json();
        })
        .then(function (snapshot) {
          backoffStep = 0;
          var status = snapshot.status || 'pending';
          updateJobProgress(snapshot);
          if (Object.prototype.hasOwnProperty.call(TERMINAL_STATES, status)) {
            if (status === 'completed') {
              // Total of envelopes the dry-run pass found. Falls
              // back to `matched` because the existing chunked job
              // hash exposes `matched` directly (the streaming
              // counter HINCRBY'd per chunk); both equal `total_found`
              // on a completed dry-run.
              var matchCount = (
                typeof snapshot.matched === 'number' ? snapshot.matched : 0
              );
              fillSynchronousResult(rootEl, {
                match_count: matchCount,
                // The chunked dry-run path doesn't surface kept_index
                // through the job hash (the synchronous count walks
                // the queue specifically to track it; the chunked
                // worker only HINCRBYs deltas). We hide the kept-index
                // row in job mode rather than show stale data.
                kept_index: null,
                sample: rootEl.__previewSample || [],
              });
            } else if (status === 'cancelled') {
              showError('Count job was cancelled. Refresh to retry.');
            } else {
              showError(
                'Count job failed' +
                (snapshot.error ? ': ' + snapshot.error : '.') +
                ' Refresh to retry.'
              );
            }
            return;
          }
          // Surface live progress so the operator knows it's
          // making forward progress on a deep queue.
          scheduleNext(POLL_INTERVAL_OK_MS);
        })
        .catch(function () {
          var step = backoffStep < POLL_BACKOFF_STEPS_MS.length
            ? POLL_BACKOFF_STEPS_MS[backoffStep]
            : POLL_BACKOFF_MAX_MS;
          backoffStep += 1;
          scheduleNext(step);
        })
        .then(function () { inFlight = false; });
    }

    poll();
  }

  function updateJobProgress(snapshot) {
    var processed = snapshot.processed || 0;
    var total = snapshot.total_estimated || 0;
    setText('celery-clear-preview-job-processed', formatNumber(processed));
    setText('celery-clear-preview-job-total', formatNumber(total));
    setText('celery-clear-preview-job-status', snapshot.status || 'pending');
    var bar = $('celery-clear-preview-job-bar');
    if (bar) {
      bar.value = processed;
      bar.max = total > 0 ? total : 1;
    }
  }

  function init() {
    var root = $('celery-clear-by-filter-preview-root');
    if (!root) return;

    if (root.dataset.countSkipped === '1') {
      // Tier 4: the operator chose to skip the count. Server
      // rendered with the count card hidden + submits unlocked;
      // nothing for us to do here.
      return;
    }

    var previewUrl = root.dataset.previewUrl;
    if (!previewUrl) return;

    setSubmitButtonsEnabled(false);

    fetch(previewUrl, {
      credentials: 'same-origin',
      headers: { 'Accept': 'application/json' }
    })
      .then(function (r) {
        if (!r.ok) {
          // Surface server-side validation / 5xx errors with the
          // detail message for the operator. JSON body shape:
          // {error, detail}.
          return r.json().then(function (payload) {
            throw new Error(
              (payload && payload.detail) || 'preview HTTP ' + r.status
            );
          }, function () {
            throw new Error('preview HTTP ' + r.status);
          });
        }
        return r.json();
      })
      .then(function (payload) {
        if (payload.mode === 'job') {
          // Stash the synchronously-fetched sample (the preview
          // endpoint runs `_materialise_sample_targets` even in
          // job mode because LRANGE 0 N is fast) so we can render
          // it once the job completes; the job hash itself only
          // tracks counts, not envelope shapes.
          root.__previewSample = payload.sample || [];
          // Reveal the job-mode loader card + start polling.
          hide($('celery-clear-preview-loader'));
          show($('celery-clear-preview-job-card'));
          pollJob(root, payload.status_url);
          return;
        }
        // Synchronous mode: fill in directly.
        fillSynchronousResult(root, payload);
      })
      .catch(function (err) {
        showError(
          (err && err.message)
            ? err.message
            : 'Could not load preview. Use the skip-count link below to proceed.'
        );
      });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
