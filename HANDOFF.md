# hcurl maintenance notes

## Non-negotiable architecture

- libcurl callbacks stay in C. Do not introduce `foreign import ... "wrapper"`
  callbacks on transfer hot paths.
- The C MPSC queue is the multi-producer control path. The libuv agent thread
  is the sole consumer and the sole owner of the transfer registry.
- Download and upload payloads use the bounded C ring in `cbits/stream.c`.
  A ready reader/writer operation makes one unsafe Haskell-to-C call. Blocking
  adds waiter registration, one `MVar` sleep, and a retry; setup and final
  snapshots are batched where state has the same lifetime.
- The package baseline is libcurl 7.18 and libuv 1.0. Streaming upload alone
  requires libcurl 7.19 because 7.18's `CURL_READFUNC_PAUSE` implementation is
  memory-unsafe; keep the linked-library runtime guard and
  `streamingUploadSupported`.
- Streaming POST uses libcurl upload state with an unknown size and a custom
  `POST` wire method. Keep the request-local `Expect:` suppression: curl 7.19.0
  can otherwise stall in the old socket API while both peers wait. For reusable
  `OverrideHeaders`, the suppression is an owned one-node overlay over the
  borrowed slist; never copy, mutate, or prematurely release the caller's list.
- `hs_try_putmvar` provides one-shot wakeups. It consumes a fired
  `newStablePtrPrimMVar` pointer; never free or reuse that pointer after C has
  detached and fired the waiter. Cancellation may free it only when the C
  unregister operation explicitly returns ownership. Easy completion tracks
  that transfer with its own atomic ownership flag; do not read the generic
  waker's plain field from a different thread.
- An easy handle belongs to Haskell until execute enqueue succeeds. From that
  point through completion, cancellation, or shutdown it belongs exclusively
  to the C agent thread.
- Resume and cancel messages carry `TransferId`, never `CURL *`. IDs are not
  reused, and late messages are no-ops.
- Managed workers stay in the pool registry while retirement is in progress.
  Stop first and remove second, so concurrent close or an async exception
  cannot orphan a reactor.
- Agent and controller threads are spawned with `asyncWithUnmask` variants.
  Their parents run masked during setup, so plain `async` would silently make
  a child-local `restore` ineffective and could make hooks unkillable.
- `CurlCode` is a header-independent numeric ABI table with an
  `UnknownCurlCode` fallback. Do not regenerate it from the local curl headers:
  that makes the public constructors vary by build and makes a newer runtime's
  error code partial at `toEnum`.

## Build gotcha

`c2hs` does not reliably notice C header layout changes, and Cabal's inplace
package registration is shared by configurations in one build directory.
After changing a C struct/header, compiler optimization, or native dependency,
use a fresh build directory before interpreting crashes or type mismatches.
Pass that same directory and configuration to `cabal list-bin`; an unqualified
`list-bin` can select a stale executable from another configuration.

```console
direnv exec . cabal test all -j1 \
  --builddir=dist-verify-o2 \
  --enable-optimization=2
direnv exec . cabal list-bin test:hcurl-test \
  --builddir=dist-verify-o2 \
  --enable-optimization=2
```

## Required release checks

- clean `-O2` build and both test suites with default RTS capabilities and
  `+RTS -N1 -RTS`;
- C compilation with `-Wall -Wextra -Werror`;
- Valgrind or equivalent lifetime checking for shutdown/cancel tests;
- `cabal check`, Haddock, sdist build, and Nix build;
- compatibility compile against the declared libcurl 7.18 and libuv 1.0
  headers after changes to native code, plus the upload suite against libcurl
  7.19.0 so the feature-specific floor remains real rather than inferred.

All `HCurl.Internal.*` modules intentionally remain exposed, but top-level
modules should present the smaller supported interface. `HCurl.Agent` is only
the abstract façade; reactor internals live in `HCurl.Internal.Agent`.
