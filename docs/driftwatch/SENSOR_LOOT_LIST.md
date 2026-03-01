# Sensor Loot List — Stealable Primitives for Driftwatch

Catalog of methods worth stealing for the sensor array.
Not a literature review — a shopping list with honest cost estimates.

## Format

Each entry: **method** → detects → inputs → failure modes → cheap approximation → do we have the data?

---

## 1. Burst / Volume Anomaly

### Kleinberg Burst States
- **Detects:** Periods of elevated activity in a stream, modeled as a hidden Markov process over Poisson rates.
- **Inputs:** Event timestamps per stream (fingerprint).
- **Failure modes:** Sensitive to rate parameter choice. Slow streams get false bursts from any activity. Doesn't distinguish organic spread from amplification.
- **Cheap approximation:** Already approximated — `compute_burst_scores()` uses z-score of latest bin vs rolling baseline. Semantically close enough for windowed operation.
- **Do we have the data?** Yes. `claim_history.createdAt` per fingerprint.

### EWMA Z-Score (current)
- **Detects:** Bins where volume exceeds rolling mean + k*stddev.
- **Inputs:** Count per bin.
- **Failure modes:** Assumes stationary baseline. Cold-start (first few bins) produces garbage z-scores. Doesn't adapt window size to signal frequency.
- **Cheap approximation:** This *is* the cheap approximation. Already in `compute_burst_scores()`.
- **Do we have the data?** Yes.

### Sliding-Window Poisson Test
- **Detects:** Windows where observed count significantly exceeds expected (Poisson CDF test).
- **Inputs:** Event counts per window, expected rate.
- **Failure modes:** Poisson assumption breaks for bursty/overdispersed data (most social streams). Negative binomial is more honest but heavier.
- **Cheap approximation:** Use Poisson CDF from stdlib `math` — `1 - poisson_cdf(k, lambda)` via incomplete gamma. Or just use the z-score we already have.
- **Do we have the data?** Yes.

---

## 2. Drift / Changepoint Detection

### ADWIN (Adaptive Windowing)
- **Detects:** Distribution shifts in a numeric stream by maintaining a variable-length window that splits when sub-windows diverge.
- **Inputs:** Ordered numeric stream (e.g., EPS, burst scores, Herfindahl index).
- **Failure modes:** Detects *any* distributional change, not just interesting ones. High false-positive rate on noisy streams without post-filtering. Memory grows with window until cut.
- **Cheap approximation:** Yes — maintain running mean/variance in two halves of a window, split when Hoeffding bound exceeded. ~20 lines of code, O(1) per observation amortized.
- **Do we have the data?** Yes — any numeric metric we already compute per window.

### BOCD (Bayesian Online Changepoint Detection)
- **Detects:** Changepoints by maintaining a posterior over run-lengths (how long since last change). Principled uncertainty.
- **Inputs:** Ordered numeric stream + prior on hazard rate.
- **Failure modes:** Run-length posterior is O(T) memory without pruning. Conjugate prior choice matters (Gaussian fine for EPS, wrong for counts). Hazard rate prior is a knob people lie about tuning.
- **Cheap approximation:** Moderate — prune run-lengths beyond 200 bins (negligible posterior mass). Use Gaussian conjugate for EPS-like streams, Poisson-Gamma for count streams. ~50 lines.
- **Do we have the data?** Yes, same as ADWIN.

### Regime Shift Detection (current)
- **Detects:** Jensen-Shannon divergence between rolling baseline and current label distribution.
- **Inputs:** Label distribution vectors per bin.
- **Failure modes:** Requires labels to exist (sparse in sealed lab). JSD is symmetric — doesn't tell you *which* direction shifted. Baseline window size is a hidden knob.
- **Do we have the data?** Yes. `label_decisions` table. But sparse until we emit labels.

---

## 3. Weak Supervision (framing, not implementing)

### Dawid-Skene Model
- **Detects:** Latent true labels given noisy annotations from multiple annotators with unknown reliability.
- **Inputs:** Annotator × item × label matrix.
- **Failure modes:** Assumes annotators are independent (never true). EM can converge to degenerate solutions. Needs many items per annotator.
- **Cheap approximation:** NOT implementing EM. Using the *framing* only: each sensor is an "annotator," each fingerprint is an "item," and we can compute per-sensor reliability scorecards (agreement rate with other sensors, false-fire rate on known-boring windows) without EM.
- **Do we have the data?** Will have once 2+ sensors run. Not yet.

### GLAD (Generative model of Labels, Abilities, and Difficulties)
- **Detects:** Same as Dawid-Skene but models item difficulty (some fingerprints are harder to classify).
- **Inputs:** Same matrix.
- **Failure modes:** Same as Dawid-Skene plus: difficulty parameter is underdetermined with few annotators.
- **Cheap approximation:** Same framing-only approach. Track per-fingerprint "how many sensors fire" as a difficulty proxy without the generative model.
- **Do we have the data?** Same — needs multi-sensor history.

---

## 4. Coordination Detection

### CopyCatch (Lockstep Bipartite)
- **Detects:** Groups of accounts that perform the same actions on the same targets in lockstep. Originally for Facebook like-fraud.
- **Inputs:** Bipartite graph (author → fingerprint) with timestamps.
- **Failure modes:** Needs actual bipartite structure — degenerates if everyone shares the same fingerprint (viral content). Threshold sensitivity. Doesn't catch slow-drip coordination.
- **Cheap approximation:** Yes — co-occurrence counts: for each pair of authors, count shared fingerprints within Δt window. Flag pairs above threshold. O(authors² × fps) per window, but cap at top-N authors.
- **Do we have the data?** Yes. `claim_history` has (authorDid, claim_fingerprint, createdAt).

### CLSB (Co-Link Sharing within Δt)
- **Detects:** Accounts sharing the same URL/domain within a tight time window.
- **Inputs:** (author, URL/domain, timestamp) triples.
- **Failure modes:** High false positives on trending URLs (everyone shares the same news). Needs URL extraction quality. Window Δt is a knob.
- **Cheap approximation:** Yes — per-domain-per-window, count distinct authors. Flag domains with unusual author concentration vs baseline.
- **Do we have the data?** Partially. `fp_kind='domain'` fingerprints capture URL domains, but we don't store raw URLs. Domain-level is fine for this.

### Behavioral Trace Similarity (Pacheco et al.)
- **Detects:** Accounts with similar posting patterns (timing, content mix, activity cycles).
- **Inputs:** Per-author activity vectors (hourly posting histogram, fingerprint type distribution, etc.).
- **Failure modes:** Profile-level analysis — conflicts with aggregate-first design constraint. Similarity metric choice matters. Expensive at scale.
- **Cheap approximation:** NOT implementing per-account profiling. But: aggregate version is feasible — track the *distribution* of posting patterns (hour-of-day histogram across all authors) and detect when it shifts (e.g., activity spike at unusual hours = time-zone anomaly).
- **Do we have the data?** Yes for aggregate version. `claim_history.createdAt` gives hour-of-day.

---

## 5. Graph Anomaly

### Density Spikes
- **Detects:** Sudden increase in edge density in a subgraph (e.g., author-fingerprint bipartite graph becomes unusually connected).
- **Inputs:** Edge counts per window, node counts per window.
- **Failure modes:** Density = edges/(nodes²) is brittle with small node counts. Viral content naturally densifies.
- **Cheap approximation:** Yes — track `edges_in_window / (distinct_authors * distinct_fps)` per window. Baseline with EWMA, flag spikes. O(1) per window with pre-computed counts.
- **Do we have the data?** Yes. Derivable from `claim_history` aggregates.

### Sudden Edge Formation
- **Detects:** New connections (author posts about fingerprint for the first time) appearing faster than baseline.
- **Inputs:** Count of first-time (author, fingerprint) pairs per window.
- **Failure modes:** High rate during breaking news (everyone is new to the topic). Needs careful baseline.
- **Cheap approximation:** Yes — `COUNT(*)` of first appearances per window. Need a "seen before" structure (Bloom filter or just `MIN(createdAt)` per author-fp pair).
- **Do we have the data?** Yes, derivable. But `MIN(createdAt)` query per pair is expensive unless pre-aggregated.

### Biclique Detection
- **Detects:** Complete bipartite subgraphs (set of authors all sharing the same set of fingerprints). Strong coordination signal.
- **Inputs:** Author-fingerprint bipartite adjacency.
- **Failure modes:** Exact biclique is NP-hard. Approximate biclique (dense subgraph) is tractable but noisy. Small bicliques (2×2) are everywhere and meaningless.
- **Cheap approximation:** Moderate — enumerate "frequent fingerprint sets" per author window, then group authors by their set. Flag groups where ≥3 authors share ≥3 fingerprints. Uses sorted hashing, no graph library.
- **Do we have the data?** Yes. `claim_history` gives (author, fingerprint) pairs.

---

## Priority Assessment

| Method | Effort | Signal Quality | Data Ready? | M3 Candidate? |
|--------|--------|---------------|-------------|----------------|
| EWMA z-score | Done | Good | Yes | Already shipping |
| Concentration (Herfindahl) | Low | High | Yes | **Yes — H1** |
| Author velocity ratio | Low | High | Yes | **Yes — H2** |
| ADWIN | Low | Medium | Yes | Good M4 candidate |
| Co-occurrence counts | Medium | High | Yes | Good M4+ candidate |
| Density spikes | Low | Medium | Yes | Good M4 candidate |
| BOCD | Medium | High | Yes | M5+ (needs tuning) |
| Behavioral traces (aggregate) | Medium | Medium | Yes | M5+ |
| CopyCatch | Medium | High | Yes | M5+ (needs multi-author data) |
| Biclique approximate | High | High | Yes | M6+ (expensive) |
| Dawid-Skene framing | Low | Meta | No (needs 2+ sensors) | After M3 |

---

## Data Availability Summary

Everything we need lives in `claim_history`:
- `(authorDid, claim_fingerprint, createdAt, fp_kind)` — covers burst, concentration, velocity, coordination
- `evidence_class` — covers evidence diversity signals
- `confidence` — covers drift in confidence distributions

`label_decisions` adds regime shift data but is sparse in sealed lab mode.

`platform_health` snapshot adds context gating (don't trust signals during degraded ingestion).

No new tables needed for M3. M4+ coordination heads will want pre-aggregated co-occurrence, but that's future work.
