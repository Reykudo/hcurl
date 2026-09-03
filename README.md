# HCurl - Network client based on libcurl - WIP

Library modules use the `HCurl` namespace, for example `HCurl.Agent`,
`HCurl.Request`, and `HCurl.Simple`.

## Streaming

`HCurl.Streaming` provides a framework-independent way to consume a response
body before the transfer has finished:

```haskell
import qualified HCurl.Agent as Agent
import HCurl.Request
import HCurl.Response (StreamingResponse (..))
import HCurl.Streaming
import Control.Monad.Trans.Resource (runResourceT)

-- readBody :: BodyReader -> IO (Either CurlCode (Maybe ByteString))
drain :: BodyReader -> IO ()
drain reader = readBody reader >>= \case
    Right (Just _) -> drain reader
    _ -> pure ()

runResourceT do
    agent <- Agent.spawnAgent Agent.defaultConfig
    result <- httpStreaming agent request
    case result of
        Left code -> print code
        Right StreamingResponse{info, body, completion} -> do
            print info.statusCode
            drain body
            completion
```

The conduit binding lives in the separate `hcurl-conduit` package:

```haskell
import qualified HCurl.Agent as Agent
import HCurl.Conduit (http)
import Data.Conduit (runConduitRes, (.|))
import Data.Conduit.Binary (sinkFile)
import Control.Monad.Trans.Resource (runResourceT)

runResourceT do
    agent <- Agent.spawnAgent Agent.defaultConfig
    Right response <- http agent request
    runConduitRes $ response.body .| sinkFile "/tmp/download.bin"
```

`bracketP` inside `bodyReaderSource` closes the body when the source is
consumed to EOF or when the enclosing `ResourceT` scope ends, so abandoning a
stream early cancels the underlying transfer at scope exit. Use
`runConduitRes` (a dedicated scope) when you need prompt cancellation after
partial consumption.

## Development

The repository uses direnv and Nix to provide the complete Haskell and C toolchain:

```console
direnv allow
nix build .#hcurl
```

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
