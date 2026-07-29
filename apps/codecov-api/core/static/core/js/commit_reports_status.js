// Lazy-loads report status icons for the Commit admin changelist after the
// initial page render. Loaded as an external file for CSP compatibility.
(function () {
  var STATUS_ICONS = {
    true: "/static/admin/img/icon-yes.svg",
    false: "/static/admin/img/icon-no.svg",
    unknown: "/static/admin/img/icon-unknown.svg",
  };

  function getCookie(name) {
    var match = document.cookie.match(new RegExp("(?:^|; )" + name + "=([^;]*)"));
    return match ? decodeURIComponent(match[1]) : "";
  }

  function renderStatusIcon(status) {
    var img = document.createElement("img");
    img.src = STATUS_ICONS[status] || STATUS_ICONS.unknown;
    img.alt = status === "true" ? "True" : status === "false" ? "False" : "Unknown";
    return img;
  }

  function loadReportStatuses() {
    var loader = document.getElementById("commit-reports-status-loader");
    if (!loader) return;

    var url = loader.dataset.statusUrl;
    if (!url) return;

    var cells = document.querySelectorAll(".commit-reports-status[data-commit-id]");
    if (!cells.length) return;

    var commitIds = [];
    for (var i = 0; i < cells.length; i++) {
      commitIds.push(cells[i].dataset.commitId);
    }

    fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCookie("csrftoken"),
      },
      body: JSON.stringify({ commit_ids: commitIds }),
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("HTTP " + response.status);
        }
        return response.json();
      })
      .then(function (payload) {
        var statuses = payload.statuses || {};
        for (var j = 0; j < cells.length; j++) {
          var cell = cells[j];
          var status = statuses[cell.dataset.commitId] || "unknown";
          cell.textContent = "";
          cell.removeAttribute("aria-busy");
          cell.appendChild(renderStatusIcon(status));
        }
      })
      .catch(function () {
        for (var k = 0; k < cells.length; k++) {
          var failedCell = cells[k];
          failedCell.textContent = "?";
          failedCell.removeAttribute("aria-busy");
          failedCell.title = "Could not load report status";
        }
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", loadReportStatuses);
  } else {
    loadReportStatuses();
  }
})();
