# HCurl - Network client based on libcurl - WIP

Library modules use the `HCurl` namespace, for example `HCurl.Agent`,
`HCurl.Request`, and `HCurl.Simple`.

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
