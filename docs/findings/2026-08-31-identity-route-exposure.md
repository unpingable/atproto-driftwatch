# Identity route exposure — deployment receipt (STOPPED)

Date: 2026-08-31
Qualified commit: `90057f979e2e6a48993aad5ec53ee426c046a632`
Deployed: **NO — stopped before deployment.**
Author of receipt: independent of the commit's authorship path; all evidence
below was gathered read-only against production, not inferred from the diff.

## Outcome

The Driftwatch identity-route repair was **not deployed**. The stop condition
stated in the deployment brief was met: `ADMIN_API_TOKEN` is unset in
production, which makes `admin_auth` a no-op. Deploying `90057f9` under those
conditions would attach `Depends(admin_auth)` decorators that admit every
caller, and would produce a route table, a README, and a test suite all
asserting a boundary that does not exist at runtime. That is a worse state
than the current one, because the current one is at least legible as broken.

## Exposure topology (B1)

Production host: single Linode, `192.46.223.21`. Caddy container fronts
`*:80` and `*:443`. Driftwatch runs in Docker publishing `127.0.0.1:8422`.

`/home/jbeck/atproto/Caddyfile`, lines 73–74:

    driftwatch.sp00ky.net {
        reverse_proxy localhost:8422

There is no path matcher, no path restriction, and no authentication
directive in this block. Compare the Labelwatch block at line 22, which does
use a path matcher in front of `localhost:8423`. The difference is not
stylistic: Driftwatch's entire FastAPI surface is proxied.

`driftwatch.sp00ky.net` resolves to `192.46.223.21`.
`https://driftwatch.sp00ky.net/health` returns HTTP 200 from off-host.

Deployed build: `GIT_SHA=d657d68`. The deployed `src/labeler/main.py` was read
directly from the running container; the three identity routes carry no
`Depends(admin_auth)`.

Route classification:

| Route | Classification | Evidence |
|---|---|---|
| `/exposure/{did}` | **PUBLIC** | reached the app from off-host; no matcher upstream; no auth in deployed code |
| `/strain/top` | **PUBLIC** | same |
| `/labels/{subject_uri}` | **PUBLIC** for slash-free subjects | same; see routing note below |
| `/admin/mappings` | **PUBLIC** (effective) | routed as above; `admin_auth` inert, see B2 |
| `/admin/cooldowns` | **PUBLIC** (effective) | same |
| `/recent-decisions` | **PUBLIC** (effective) | same |
| `/quarantine/recent` | **PUBLIC** (effective) | same |
| `/health` | PUBLIC (intended) | HTTP 200 off-host |

Probing was confined to topology: `/health` for reachability, and an
obviously nonexistent identity value for the per-DID routes. No real subject
was enumerated and no response body containing subject data was retrieved or
stored.

Behavioural note: the three identity routes do not refuse and do not 404 —
they **hang**. External requests reached the application and returned curl
exit 28 after 40s with 0 bytes, on unbounded SQL. Reachability is therefore
established by the request being accepted and served, not by a status code.

Routing quirk, deliberately **not** repaired in this campaign: an `at://`
subject URI contains slashes, so it never matches the single-segment
`/labels/{subject_uri}` route and 404s at routing before any auth dependency
runs. This is pinned by `test_at_uri_subject_does_not_route` so that a later
path-routing fix cannot silently open an unauthenticated hole.

## ADMIN_API_TOKEN (B2)

**Effective: NO.** Confirmed three independent ways, all read-only:

1. The running container's environment does not define `ADMIN_API_TOKEN`.
2. `/opt/driftwatch/deploy/.env` — 0 matching lines.
3. `/opt/driftwatch/deploy/.env.prod` — 0 matching lines.

Mechanism: `admin_auth` reads the token from configuration and, when no token
is configured, admits unconditionally. This no-op-when-unset behaviour is not
an inference; it is pinned by the existing test
`test_admin_auth_is_a_noop_without_a_configured_token`.

No secret value was printed, copied, or stored. Only set/unset status, the
mechanism, and the config provenance above are recorded.

## Severity adjudication (B3)

**Externally reachable access-control defect.** Not latent divergence, not
defence-in-depth, not documentation drift.

The prior state was not "identity routes exist but are only reachable from
loopback." The routes are proxied to the public internet under a hostname
that resolves publicly and answers publicly. `docs/architecture/PUBLIC_SURFACES.md`
already forbids per-DID surfaces and forbids answering dossier-shaped
questions from the API even where the tables could. Production contradicted
that doctrine directly.

Scope is larger than the three routes named in the campaign. Because
`ADMIN_API_TOKEN` is unset, `admin_auth` protects nothing today, so
`/admin/mappings`, `/admin/cooldowns`, `/recent-decisions` and
`/quarantine/recent` are also effectively open. This finding was not part of
the original Breakwater scope and is recorded here because it materially
changes the operator action required.

Known consumers of the identity routes: **none identified.** No client code
in the tree calls them; the dashboard does not.

## Smallest required operator action

Set `ADMIN_API_TOKEN` to a strong random value in
`/opt/driftwatch/deploy/.env` and restart the container so the running
process picks it up.

That single action is worth doing **on its own, before and independently of
deploying `90057f9`**, because it closes the currently-open `/admin/*`,
`/recent-decisions` and `/quarantine/recent` surfaces immediately. Deploying
`90057f9` afterwards then extends the same boundary over the three identity
routes and makes the repair real.

Optionally, and separately: add a path matcher to the `driftwatch.sp00ky.net`
Caddy block so the public hostname proxies only the surfaces
`PUBLIC_SURFACES.md` actually sanctions. That is a second layer, not a
substitute — a loopback-bound identity route is still an identity route, and
so is a Caddy-hidden one.

## What was NOT done

- `90057f9` was not deployed.
- `admin_auth` was not redesigned.
- The `at://` path-routing issue was not fixed.
- No secret was written into this receipt.
- The unrelated in-flight retention/ops-visibility work in the worktree was
  not touched.
