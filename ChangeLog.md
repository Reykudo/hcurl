# Changelog for hcurl

## Unreleased changes

- Lowered native requirements to libcurl 7.18 and libuv 1.0. Newer optional
  curl settings now fail at their setup/use site when unsupported instead of
  becoming package-wide build-time requirements.
- Replaced synchronous Haskell callbacks with C-only download, upload, and
  header callbacks. Final metrics are captured once at completion instead of
  running a progress callback. Bounded C streams use one-shot MVar wakeups only
  when a Haskell reader or writer is actually blocked. Ready chunk operations
  use one unsafe Haskell-to-C call, and completion state is extracted in one
  batched snapshot.
- Added bounded streaming POST uploads with backpressure, explicit EOF/abort,
  and redirect replay protection. Upload streaming requires libcurl 7.19;
  libcurl 7.18's memory-unsafe pause implementation is rejected at runtime,
  while all non-upload-streaming functionality retains the 7.18 minimum.
  HTTP/1.1 framing uses libcurl's unknown-size upload mode and suppresses its
  implicit `Expect: 100-continue` handshake unless callers request it.
- Made transfer ownership explicit with monotonic IDs and an agent-thread-only
  C registry. Late cancel/resume messages are safe, easy handles are destroyed
  on their owning reactor, and ID wraparound cannot reuse an old ID.
- Added idempotent `closeAgent` plus `withAgent`, `withThreadedAgent`, and
  `withManagedAgent`. Closing aborts active transfers and managed shutdown now
  terminates its controller even when a metrics hook is blocked. Threaded pools
  skip stopped workers; managed pools replace failed workers and retain
  ownership of retiring workers until their reactors have actually stopped.
  Reactor and controller threads now receive an explicit unmask function, so
  parent-side setup masking cannot make user hooks or loop waits unkillable.
- Buffered request bodies now work for GET, POST, PUT, DELETE, PATCH, and custom
  methods and are copied into libcurl before submission.
- Response streaming now publishes completed headers before body data and keeps
  informational, redirect, authentication, and proxy blocks separate.
- Added typed request/option validation, 64-bit metrics with documented units,
  setup error checking, and stable ownership for overridden header lists.
  Streaming uploads reject an explicit redirect-following option instead of
  silently overriding it, and normal completion no longer closes the upload
  stream through the cancellation path.
- Made `CurlCode` independent from the build headers and added
  `UnknownCurlCode` for errors introduced by future linked runtimes. Metrics
  select modern 64-bit or legacy `getinfo` queries from the runtime version,
  not only the compile-time headers.
- Renamed `Request.host` to `Request.url`, reduced the public request interface,
  added `defaultRequest`, and removed the post-completion dangling easy handle
  from request state. Added public `HCurl.Options`, `HCurl.Headers`, and
  `HCurl.Metrics` façades while keeping every `HCurl.Internal.*` module exposed.
- Removed the unused `HCurl.PyFCustom` formatter and the shallow
  `HCurl.Extras` pointer/debug interface.
- Completed cleanup keys are removed from long-lived `ResourceT` scopes, and
  duplicate stream close/abort calls no longer cross into C repeatedly.
- Added local TCP regression coverage for lifecycle races, async waiter
  cancellation, redirects, all request methods, upload/download backpressure,
  duplicate IDs, and clean agent teardown.
- Corrected the Cabal license metadata to match the repository's existing
  Apache-2.0-or-MIT license files and README grant.
- Store managed-worker primitive counters in `IORefU` while retaining boxed
  references where atomic `Word64`, `Bool`, or higher-level values are needed.
  The existing managed-state `MVar` remains the synchronization boundary.
