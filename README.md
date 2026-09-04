# hcurl

`hcurl` is an experimental Haskell HTTP client built around libcurl's multi
socket interface and libuv. It supports buffered requests, bounded streaming
responses, bounded streaming POST bodies, and fixed or dynamically managed
reactor pools.

## Requirements

- libcurl 7.18 or newer
- libuv 1.0 or newer
- GHC 9.10 (the current Cabal bounds require `base >= 4.20.2`)

Streaming POST bodies require libcurl 7.19 or newer. libcurl 7.18 exposes the
pause constant but its upload-pause implementation is memory-unsafe; hcurl
detects that version and throws `StreamingUploadUnsupported` instead of
entering the broken path. Buffered requests and response streaming retain the
7.18 minimum.

The low C-library bounds are intentional. Features introduced by newer
libcurl versions, such as `CURLOPT_PIPEWAIT` or TCP Fast Open, remain available
through `SomeOption`; selecting one when it is unavailable in the build headers
or linked runtime returns a curl setup error instead of raising the package-wide
minimum.
Non-zero `maxConnection` or `maxConnectionPerHost` settings require libcurl
7.30; on older libcurl they fail agent initialization instead of being silently
ignored. The default zero values work on the declared 7.18 minimum.

`CurlCode` is defined by hcurl's stable numeric table rather than generated
from the installed headers. Codes added by a newer linked libcurl are reported
as `UnknownCurlCode number` instead of crashing a decoder built with older
headers. Completion metrics similarly fall back to legacy `getinfo` queries
when the linked runtime predates the newer 64-bit query variants.

All `HCurl.Internal.*` modules remain exposed for low-level users. Their
interfaces are less stable than the top-level modules.

The supported public interface is split by role: `HCurl.Agent`,
`HCurl.Request`, `HCurl.Options`, `HCurl.Headers`, `HCurl.Response`,
`HCurl.Metrics`, `HCurl.Simple`, `HCurl.Streaming`, and `HCurl.Upload`.
`HCurl.Agent` deliberately keeps `Agent` abstract; the complete reactor
implementation remains available from exposed `HCurl.Internal.Agent`.

## Reactor and callback model

Each worker owns a libuv loop, a libcurl multi handle, and an agent-thread-only
registry keyed by a monotonically increasing `TransferId`. Once an execute
message has entered the C MPSC queue, the worker owns and eventually destroys
the easy handle. Cancel and resume messages contain only the transfer ID, so a
late control message is a safe no-op and cannot dereference a stale `CURL *`.

libcurl's body, upload, and header callbacks are implemented in C.
They never synchronously enter Haskell. Download and upload data pass through
the same bounded C ring-buffer implementation. A blocked Haskell operation is
woken by a one-shot `hs_try_putmvar`; there is no Haskell callback or pair of
background Haskell threads per streaming transfer. In the normal streaming hot
path, Haskell crosses the FFI only when the caller explicitly reads or feeds a
chunk (plus setup, completion, and requested snapshots). A ready `readBody` or
`feedBody` operation is one unsafe Haskell-to-C call. A blocked operation
registers a one-shot waiter, sleeps on its `MVar`, and retries; it never polls
through a Haskell callback.

Final transfer metrics are collected once on the reactor immediately before
the easy handle is destroyed; no progress callback runs during a request.
Buffered completion copies curl status, body, headers, HTTP status, and metrics
through one C snapshot call; streaming completion snapshots status and metrics
through one call.

## Agent lifetime

Prefer the bracketed constructors. `closeAgent` is concurrent-safe and
idempotent, rejects later requests with `AgentClosed`, and completes active
transfers with `AbortedByCallback`.

```haskell
import Control.Monad.Trans.Resource (runResourceT)
import qualified HCurl.Agent as Agent
import HCurl.Request (defaultRequest)
import HCurl.Simple (httpLBS, initCurl)
import HCurl.Types (defaultConfig)

main :: IO ()
main = do
    initCurl
    Agent.withAgent defaultConfig \agent -> do
        result <- runResourceT $ httpLBS agent (defaultRequest "https://example.com/")
        print result
```

`withThreadedAgent` starts a fixed pool. `withManagedAgent` starts a pool whose
growth happens at request admission and whose idle workers are retired by a
controller. Every constructor has a matching explicit `closeAgent` path.

## Requests and options

`Request.url` is a complete URL. Buffered bodies work with `GET`, `POST`,
`PUT`, `DELETE`, `PATCH`, and validated custom methods; the body is copied into
libcurl before submission, so its `ByteString` need not be retained by the
caller. A `HEAD` request with a body is rejected.

The high-level `SomeOption` type intentionally exposes a small,
version-stable subset of libcurl options. Options are applied after defaults,
so the last request option controls redirects, timeouts, TLS verification, and
the other supported settings. URLs, methods, headers, numeric ranges, and
string options are validated before submission.

Use `defaultRequest url` for a GET with libcurl's timeout defaults, then update
only the fields needed by the call. `HCurl.Headers.toHeaderSlist` creates an
opaque reusable list for `OverrideHeaders` without importing an internal
module.

## Streaming responses

`HCurl.Streaming` publishes the final-looking response header block before the
first body byte. Informational, authentication, proxy-CONNECT, and redirect
blocks are not mixed into the final response.

```haskell
import Control.Monad.IO.Class (liftIO)
import HCurl.Response (StreamingResponse (..))
import HCurl.Streaming

drain :: BodyReader -> IO ()
drain reader = readBody reader >>= \case
    Right (Just chunk) -> use chunk >> drain reader
    Right Nothing -> pure ()
    Left curlCode -> print curlCode

runResourceT $
    withHttpStreaming agent request \StreamingResponse{body} ->
        liftIO $ drain body
```

The queue is bounded by chunks (`StreamConfig`). `readBody` applies
backpressure when the queue is full. Consume a `BodyReader` inside its
`ResourceT` scope, or use `withHttpStreaming`; leaving the scope closes an
abandoned transfer deterministically. `completion` should be awaited after, or
concurrently with, draining the body because an unread bounded body can pause
libcurl.

The separate `hcurl-conduit` package adapts a `BodyReader` to a Conduit source.
Run a partial pipeline in its own `runConduitRes` scope when prompt early
cancellation matters.

## Streaming POST bodies

`HCurl.Upload` feeds a POST body through the same bounded C stream used by the
download path:

```haskell
import Control.Monad.IO.Class (liftIO)
import HCurl.Upload

result <- runResourceT $
    httpUpload agent postRequest \upload -> liftIO do
        feedBody upload chunk1 >>= either print pure
        feedBody upload chunk2 >>= either print pure
        -- Returning from this callback sends EOF automatically.
```

`feedBody` blocks while the queue is full. `endBody` can send EOF early and is
safe to repeat; `abortBody` cancels the transfer. HTTP/1.1 uses chunked transfer
encoding because the length is unknown. hcurl also suppresses libcurl's
automatic `Expect: 100-continue` header unless the caller explicitly supplies
one; this avoids an extra round trip and an old socket-API timeout stall.
Streaming uploads deliberately disable redirect following: replaying a
one-shot producer for 307/308 would otherwise silently duplicate or truncate
the request body. Supplying `OptionFollowLocation True` to a streaming upload
is rejected during validation rather than silently overridden.

The request passed to `httpUpload` must use `Post` with `Empty`; use
`httpUploadWith` to choose the bounded queue length. The response is currently
buffered. `streamingUploadSupported` lets applications probe the libcurl 7.19
upload minimum before constructing a streaming request.

## Development

```console
direnv allow
direnv exec . cabal test all -j1 --enable-optimization=2
nix build .#hcurl
```

The tests use real local TCP servers and cover request methods, header blocks,
redirect policy, download/upload backpressure, async waiter cancellation,
agent shutdown races, managed-pool retirement, and stale transfer IDs.

## License

Licensed under either Apache-2.0 or MIT, at your option. See
`LICENSE-APACHE` and `LICENSE-MIT`.
