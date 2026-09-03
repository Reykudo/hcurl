# Handoff: hcurl audit session (2026-09-03)

## State

- Repo: `/home/rafael/Projects/hcurl`, branch `master`.
- HEAD: `192f4dd`; worktree clean.
- Gates green at HEAD (run from repo root, inside `direnv exec .`):
  - `cabal test all -j1 --test-show-details=direct` (hcurl-test 14/14, hcurl-conduit-test 7/7)
  - hcurl-test under `--test-options='+RTS -N1 -RTS'`: 14/14
  - Full hcurl-test under valgrind `-N1`: 0 errors

## Committed this session (oldest → newest)

- `39ed4ca` managed agent pool (admission growth, controller shrink, metrics hook, agent shutdown), conduit package, streaming core + tests
- `1264fad` deterministic single idempotent stream release registered in caller's ResourceT; `withHttpStreaming`/`withHttpStreamingWith`; abandoned/blocked-reader/handler-scope tests
- `4b059fe` single registered teardown key; cancel of never-active easy wakes owner; blocked-reader and managed lease-leak tests
- `766c8d6` size-overflow guards in `simple_string_writefunc` and `header_callback` (return 0 instead of `-1`/wraparound)
- `8848977` test: `bufferedChunks = 0` rejected before transfer
- `1ed58ba` buffered body buffer grows geometrically (was O(n²) realloc per chunk)
- `192f4dd` documents why the waker stable pointer is never freed

## Known limitation (by design, documented in code)

Per-request stable pointers for `hs_try_putmvar` wakers (`doneRequest`, cancel
ack) are never freed. GHC 9.10 offers no cleanup API for
`newStablePtrPrimMVar`; freeing or reusing the pinned slot crashes the RTS
under concurrency (six strategies tested on clean rebuilds). GHC's own sample
leaks identically. Measured impact is negligible (~bytes/request after
warm-up, RSS flat over 20k requests).

## Verification coverage (done, no findings)

- Real HTTP server (Node): Content-Length, chunked, no-Content-Length,
  redirect (302), ~140 KB of headers — buffered and streaming paths OK.
- Valgrind: buffered concurrency, buffer-growth path, cancel-heavy streaming,
  conduit suite, managed (growth/cancel/abandon/start-fail/shrink), full
  hcurl-test.
- Soak: suite 16/16 runs (default and `-N1`); 100×16 concurrent requests at
  `-N4`/`-N1`; 20k sequential requests with flat RSS.
- Allocation profile: ~3 bytes allocated per body byte streaming; max
  residency ~2 MB; no red flags.

## Gotchas learned (important for future sessions)

1. **c2hs does not track C header dependencies.** Changing a struct layout in
   `cbits/*.h` leaves the generated `sizeOf` stale (we saw `sizeOf = 16`
   while C wrote 24 bytes → heap corruption). After header layout changes run
   `touch src/HCurl/Internal/Raw/SimpleString.chs` (or clean rebuild). A
   comment to this effect lives in `SimpleString.chs`.
2. **Incremental builds produced false crashes/hangs.** Many "regressions"
   during the session were stale-object artifacts. Always `cabal clean` before
   judging a change.
3. **`BodyReader` must be consumed inside its `ResourceT` scope** (or via
   `withHttpStreaming`). Reading after scope exit returns a deterministic
   `Left AbortedByCallback`; that is by design.
4. **`hs_try_putmvar` is asynchronous.** Do not free/reuse the waker stable
   pointer (see known limitation).
5. Valgrind/gdb are not in the dev shell; usable store paths:
   `/nix/store/ad60badgvym8ggmn2yxvqqyf0m3jghlw-valgrind-3.26.0/bin/valgrind`,
   `/nix/store/p6jfx09kvyy1vyi6m2flsjqcxmbgs16m-gdb-17.2/bin/gdb`.

## Suggested next steps

- Re-run the gates above after any future change.
- If a "no Content-Length streaming stall" is ever reported against a real
  server, investigate head publication (currently tied to the first body
  write); local toy-server reproductions of this were harness artifacts and
  real-server tests passed.
- Revisit the waker stable-pointer lifetime only if upstream GHC provides a
  cleanup API (`hs_try_takemvar`-like) or if the wake channel is redesigned.
