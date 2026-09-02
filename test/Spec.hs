import HCurl.Types

main :: IO ()
main
    | defaultConfig == AgentConfig{maxConnection = 0, maxConnectionPerHost = 0, connectionCacheSize = 0} =
        putStrLn "HCurl module smoke test passed"
    | otherwise = fail "HCurl default configuration is invalid"
