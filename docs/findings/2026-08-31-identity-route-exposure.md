# Identity route exposure — incident receipt (CLOSED)

Date: 2026-08-31
Status: **CLOSED**
Scope: the live exposure of the Driftwatch FastAPI app on port 8422 and the
production ingress/authentication around it. Nothing else.

## Root cause

Two independent failures, either of which alone would have contained the other.

1. **The application failed open.** `admin_auth` returned `True` when
   `ADMIN_API_TOKEN` was unset:

       token = os.getenv("ADMIN_API_TOKEN")
       if not token:
           log.debug("admin auth: no ADMIN_API_TOKEN configured; allowing open access")
           return True

   `ADMIN_API_TOKEN` had never been set in production, so the boundary was
   inert. The only signal was a `DEBUG` log line.

2. **The edge proxied everything.** The Caddy vhost was a bare
   `reverse_proxy localhost:8422` with no path matcher, so the entire
   application surface was reachable at `https://driftwatch.sp00ky.net`.

Both arrived in the initial scaffold commit `dcd8293` (2026-02-25), whose
message declares `Caddy reverse proxy at driftwatch.sp00ky.net` and
`Sealed lab posture` together. "Sealed" described *emission* — detect-only, no
public labels. It never described *ingress*. A fail-open default is defensible
in a lab nothing can reach; this was never such a lab.

Consequently seven routes answered unauthenticated requests from the public
internet: `/admin/mappings`, `/admin/cooldowns`, `/recent-decisions`,
`/quarantine/recent`, `/exposure/{did}`, `/strain/top`, `/labels/{subject_uri}`.
The four admin-ish routes returned empty containers. The three per-DID identity
routes query real observation tables and did not refuse — they accepted the
request and hung on unbounded SQL. `docs/architecture/PUBLIC_SURFACES.md`
classifies per-DID surfaces as forbidden to publish; the doctrine was written
and never enforced in code.

No evidence of exploitation was found. Caddy access logs over the retained
window show two requests to this vhost: one operator probe and one
opportunistic scanner hitting `/`.

## Remediation performed

**1. Token set durably.** A 64-hex-character token was generated with
`openssl rand -hex 32` and written to `/opt/driftwatch/deploy/.env`, which is
the `env_file` referenced by `docker-compose.prod.yml`. Prior `.env` preserved
as `.env.bak.20260831T185142Z`. The value appears in no commit, no receipt, and
no log.

**2. Application now fails closed.** `admin_auth` raises `503 admin
authentication is not configured` when `ADMIN_API_TOKEN` is unset. Startup logs
`CRITICAL` when the token is missing rather than `DEBUG` while admitting
everyone. Token comparison is now constant-time (`hmac.compare_digest`).

**3. Identity routes are behind the boundary.** `/exposure/{did}`,
`/strain/top` and `/labels/{subject_uri}` carry `Depends(admin_auth)`.

**4. Edge restricted.** The `driftwatch.sp00ky.net` vhost now proxies only
`/health`; every other path is refused by Caddy with a 404 before reaching the
application.

Three tests that asserted the fail-open contract were inverted deliberately;
two admin endpoint tests now configure a token so they exercise the handlers
rather than the refusal.

## Commits deployed

Production ran `d657d68`, a merge on `deploy/2026-08-28-combined` carrying ten
commits — retention, disk-brake and recovery work from the August volume
exhaustion — that were **not** in the Breakwater branch. The previously
qualified `90057f9` was cut from `6525e5e` (2026-08-21), which is behind
production; deploying it as-is would have reverted all ten. It was instead
cherry-picked onto the live deployment.

Branch `incident/2026-08-31-identity-route-exposure`, based on `d657d68`:

| Commit | Change |
|---|---|
| `502d3b1` | cherry-pick of `90057f9` — per-DID routes behind `admin_auth` |
| `c592e4c` | fail-closed `admin_auth`, constant-time compare, startup CRITICAL, tests, README |

Deployed image `GIT_SHA=c592e4c`. Only `src/labeler/main.py` differs from
`d657d68` under `src/`, so the image delta is exactly the auth change.

`scripts`-free deploy path: `/opt/driftwatch` is not a git checkout; the image
builds from `COPY src/`. `src/labeler/main.py` was copied from the clean
worktree, syntax-checked on the host, then
`docker compose -f docker-compose.prod.yml -f docker-compose.override.yml build`
and `up -d`. The heavily annotated `docker-compose.override.yml` was not
modified. Prior `main.py` preserved as
`/root/driftwatch-rollback-20260831T185826Z.tar.gz`.

## A latent config-drift hazard found and fixed

The first Caddy reload silently did nothing. The Caddyfile is bind-mounted as a
**single file**, so the container was pinned to inode `262315` while the host
path had become inode `262388`. Host edits had not reached Caddy, and
`caddy validate` inside the container was validating the stale copy.

Worse, the container's copy was *newer* than the host file (2026-08-25 vs
2026-08-24) and contained a `juniper-pds.sp00ky.net` vhost block — four
hostnames proxying to `127.0.0.1:3001`, verified live and serving — that the
host file did not have. Restarting Caddy against the host file would have
deleted that vhost.

The live container config was therefore captured as the authoritative base, the
driftwatch restriction applied to *it*, the result written to the host path and
validated in a throwaway container against the real file, and only then was
Caddy restarted. The mount now resolves to inode `262388` on both sides.
Backups: `Caddyfile.bak.20260831T190053Z`,
`Caddyfile.bak.preReconcile.20260831T190330Z`, and the captured live config
`Caddyfile.live.20260831T190330Z`.

## Service and restart evidence

    driftwatch  recreated 18:52Z (token), rebuilt+recreated 19:00Z (GIT_SHA=c592e4c)
    docker exec driftwatch: GIT_SHA=c592e4c, ADMIN_API_TOKEN=SET
    caddy       restarted 19:01Z, config inode host=262388 cont=262388
    caddy startup: no errors

## Public verification

`https://driftwatch.sp00ky.net`, unauthenticated, from off-host:

| Route | Before | After | Refused by |
|---|---|---|---|
| `/health` | 200 | **200** | — (intentional liveness) |
| `/health/extended` | 200 (2287B) | **404** (9B) | Caddy |
| `/metrics` | 200 (8611B) | **404** (9B) | Caddy |
| `/` | 404 (app) | **404** (9B) | Caddy |
| `/admin/mappings` | 200 | **404** (9B) | Caddy |
| `/admin/cooldowns` | 200 | **404** (9B) | Caddy |
| `/recent-decisions` | 200 | **404** (9B) | Caddy |
| `/quarantine/recent` | 200 | **404** (9B) | Caddy |
| `/exposure/{did}` | hung 40s on SQL | **404** (9B) | Caddy |
| `/strain/top` | hung 40s on SQL | **404** (9B) | Caddy |
| `/labels/{subject}` | hung 40s on SQL | **404** (9B) | Caddy |

All refusals are 9 bytes — Caddy's, not the application's. Requests no longer
reach the app.

Application boundary verified independently on loopback `127.0.0.1:8422`, so
the two layers are confirmed separately rather than one masking the other:

    no token,   /admin/mappings                401  (1.7ms)
    no token,   /recent-decisions              401  (1.3ms)
    no token,   /strain/top                    401  (1.2ms)
    no token,   /exposure/{did}                401  (1.3ms)
    wrong token,/admin/mappings                401
    valid token,/admin/mappings                200
    valid token,/recent-decisions              200

The identity routes now refuse in ~1ms rather than executing unbounded SQL.

All thirteen other vhosts verified serving after the Caddy restart, including
the three `juniper-pds` hostnames.

## Residual limitations

1. `/health` remains publicly reachable by design. It returns 15 bytes of
   liveness status and no observational data.
2. The Caddyfile is a single-file bind mount. The mount is correct right now,
   but any editor that replaces the file rather than truncating it will
   re-orphan the container's view, silently. Mounting the parent directory
   would remove the failure mode.
3. The Caddyfile is not under version control, which is how the host copy
   drifted behind the running config without anyone noticing.
4. The admin token lives in plaintext in `/opt/driftwatch/deploy/.env`,
   readable by the deploying user. There is no secret manager on this host.
5. `/labels/{subject_uri}` still 404s at routing for `at://` subjects, because
   the URI contains slashes and never matches the single-segment route. Pinned
   by `test_at_uri_subject_does_not_route`. Not repaired here; it is now moot
   externally since the route is not proxied, but a future path-routing fix
   must not reopen an unauthenticated path.
6. Two tests fail in the verification environment (`test_label_ingest`,
   `test_per_did_cooldown`) because `pytest-asyncio` is absent. Pre-existing and
   unrelated; 509 passed.
7. This branch is not merged to the deployment line. Production runs
   `c592e4c` from `incident/2026-08-31-identity-route-exposure`; whoever owns
   the release line should fold it in so the next deploy does not regress it.
