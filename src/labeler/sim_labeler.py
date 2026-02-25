"""A tiny simulated labeler HTTP service used for demos.

Behavior:
- GET /xrpc/com.atproto.label.queryLabels?uri=<uri>
  - Returns 200 with JSON labels normally
  - Can be configured to return 429 for the first N requests, via env SIM_LABELER_RATE_LIMIT_COUNT
  - When returning 429, it sets Retry-After header (seconds)

This keeps the demo self-contained and allows us to exercise rate-limits, retries, and cooldowns.
"""
import os
import time
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

app = FastAPI(title="Simulated Labeler")

# number of initial requests to throttle per "uri" (if set)
SIM_RATE_LIMIT_COUNT = int(os.getenv("SIM_LABELER_RATE_LIMIT_COUNT", "2"))
# Retry-After seconds to return on 429
SIM_RETRY_AFTER = int(os.getenv("SIM_LABELER_RETRY_AFTER", "2"))

# simple in-memory counter per-subject
_counters = {}


@app.get("/xrpc/com.atproto.label.queryLabels")
async def query_labels(uri: str):
    # count requests per uri
    c = _counters.get(uri, 0) + 1
    _counters[uri] = c

    # If under the SIM_RATE_LIMIT_COUNT, return 429
    if SIM_RATE_LIMIT_COUNT and c <= SIM_RATE_LIMIT_COUNT:
        headers = {"Retry-After": str(SIM_RETRY_AFTER)}
        return Response(status_code=429, headers=headers)

    # Otherwise return a small labels list with a synthetic labeler DID
    labels = [{"labeler": "did:sim:1", "val": "demo:tag", "time": time.strftime('%Y-%m-%dT%H:%M:%SZ')}]
    return JSONResponse({"labels": labels})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8081)))
