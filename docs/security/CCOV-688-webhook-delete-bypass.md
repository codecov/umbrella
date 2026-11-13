# Security report: unauthenticated DELETE reaches the GitHub webhook handler

**Status:** live on `api.codecov.io` at time of writing (2026-11-04)
**Reference:** CCOV-688, PR #2667, issue #2677
**Reporter:** tsabita.ac@gmail.com · Source IP: 103.147.32.6

## Summary

`https://api.codecov.io/webhooks/github_enterprise` accepts unauthenticated HTTP
`DELETE`. The request passes framework method routing and reaches
`GithubWebhookHandler.delete`, bypassing `validate_signature`.

## Evidence

```
$ curl -X DELETE https://api.codecov.io/webhooks/github_enterprise
{"error": "Server Error (500)"}

$ curl -X POST https://api.codecov.io/webhooks/github_enterprise
{"detail":"You do not have permission to perform this action."}

$ curl https://api.codecov.io/webhooks/github_enterprise
{"detail":"Method \"GET\" not allowed."}
```

The three responses differ, and the difference is the finding:

- **POST** returns a permission error. Signature validation is working correctly.
- **GET** returns a 405 at the framework layer. The view does not accept GET.
- **DELETE** returns a 500 Server Error. It is not a 405 and it is not the
  permission error, so the request reached the handler and failed inside it.

## Root cause

DRF routes HTTP `DELETE` to the `delete` method. That method was written to
handle the GitHub `delete` *event*. Because the name collides with the HTTP verb,
unsigned `DELETE` requests skip `validate_signature` entirely.

This is the failure mode described in PR #2667 / issue #2677.

## Impact

Per the PR description, the handler can hard-delete `Branch` rows. The reporter
did **not** attempt to delete anything — one unsigned `DELETE` with no body and
no target, which returned a 500 rather than a clean rejection.

1. An unauthenticated, unsigned request reaches a handler that is supposed to be
   signature-gated. The signature check is bypassed, not merely failing.
2. With a crafted body, the PR states the outcome is hard deletion of `Branch`
   rows — destructive, with potential impact on coverage data integrity.
3. Reachable by anyone on the internet with a single curl command.

Note that `/webhooks/github` behaves differently (`DELETE` returns 404 there,
`POST` returns 500), so the two endpoints are inconsistent.
`github_enterprise` is the one exhibiting the bypass.

## Reproduction

```bash
curl -X DELETE -i https://api.codecov.io/webhooks/github_enterprise
```

Expected after #2667 ships: `405 Method Not Allowed`.
Observed: `500 Server Error`, i.e. the handler is reached.

## Related finding: production is 62 commits behind main

`api.codecov.io/` returns its deployed version unauthenticated:

```
$ curl -s https://api.codecov.io/
26.7.6 is live!
```

`main` is at `26.9.10` — 62 commits ahead, including at least nine
dependency-CVE fixes (urllib3, requests, idna, pip, cryptography, lxml,
sqlparse, grpc, starlette, libexpat1 / CVE-2026-45186) and #2612 (GCP auth
credentials out of Docker images). The version disclosure is what makes that lag
actionable, so the two findings compound.

## Suggested remediation

- Merge #2667 / #2677.
- Interim: an ingress or WAF rule rejecting `DELETE` on `/webhooks/*` closes the
  exposure until it ships.
- Audit whether any other handler method name collides with an HTTP verb
  (`put`, `patch`, `get`, `head`, `options`, `trace`) the same way.

## Testing limits

One unsigned `DELETE` with no body. No data deleted, no authentication, no
repository access, nothing modified. All other requests were unauthenticated
GETs to the API root and the public GitHub API.

## Delivery note

`security@codecov.io` — the address published in `security.txt`, on
`about.codecov.io/security/`, and in `codecov/.github/SECURITY.md` — bounces
with `550 5.1.1 The email account that you tried to reach does not exist`. Five
emails were sent across three addresses and all failed. This PR is the fallback
channel.
