# Optimization Review for `plex_cli.py`

## High-impact opportunities

1. **Add request/result caching for repeated full-library scans.**  
   Commands repeatedly call `libraries()`, `all_items_by_library()`, and `all_media_rows()`; many commands chain these calls in one run. Caching these results for a short TTL (for example 5–30 seconds) would reduce API round-trips and make interactive usage much faster.

2. **Batch/paginate large section fetches instead of loading everything at once.**  
   Calls such as `/library/sections/{id}/all` are used heavily and can return very large payloads. Add `X-Plex-Container-Start`/`X-Plex-Container-Size` paging and stream/process incrementally to reduce memory spikes and latency on large libraries.

3. **Centralize “scan all libraries” workflows.**  
   Multiple commands independently loop over all libraries and fetch section results. A shared iterator helper (for libraries/items/media rows) can reuse paging, caching, and filtering behavior and eliminate duplicate work.

4. **Parallelize independent library requests.**  
   Where commands fan out per library (e.g., global search, collection aggregation, media analysis), use a bounded `ThreadPoolExecutor` to fetch sections concurrently. Plex API calls are network-bound; this can significantly improve wall-clock time.

5. **Pre-index for repeated text operations.**  
   Searches and duplicate/title checks repeatedly do lowercase transformations and scans. Build temporary indexes (`title_lower`, `(title, year)` maps, `ratingKey -> metadata`) once per command invocation.

## Medium-impact opportunities

6. **Avoid repeated `server_info()` and host parsing in playback commands.**  
   `play_media()` resolves server info and host details every call. Memoize server identity and parsed host/port once per client session.

7. **Reuse computed library list within commands.**  
   Several command paths call `self.client.libraries()` multiple times in the same flow. Keep one local `libs` variable and pass it through helper methods.

8. **Move heavy report commands to shared data snapshots.**  
   Report-style commands (`report`, storage/codec/stats variants) can reuse one snapshot of items/media rows for the command run rather than rehydrating from API in each sub-step.

9. **Compile regexes once (already partly done) and avoid duplicate splits.**  
   `clean_title()` currently splits title multiple times; store segments once and reuse.

10. **Reduce table rendering overhead for very large outputs.**  
   For huge result sets, cap default display rows and offer `--all` or `--limit` to avoid expensive rendering and slow terminal output.

## Reliability + performance hygiene

11. **Add selective retry/backoff for transient network errors.**  
   For idempotent GETs, limited retries with exponential backoff reduce user-facing failures and repeated manual reruns.

12. **Add simple profiling hooks.**  
   Optional timing logs per command (`--profile`) can identify slow paths in real library environments and verify optimization impact.

## Suggested implementation order

1. Add lightweight in-memory TTL cache for `libraries`, `all_items_by_library`, `all_media_rows`.
2. Add pagination helpers and refactor heavy fetch methods to use them.
3. Add bounded concurrency for per-library fan-out fetches.
4. Refactor top 5 slowest commands to use shared snapshot/index helpers.
5. Add profiling flag and tune defaults.
