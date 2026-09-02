{
  description = "HCurl - Haskell client based on libcurl";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-parts.url = "github:hercules-ci/flake-parts";
    pre-commit-hooks-nix.url = "github:cachix/pre-commit-hooks.nix";
    haskell-flake.url = "github:srid/haskell-flake";
  };

  outputs = inputs @ {
    self,
    flake-parts,
    nixpkgs,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = nixpkgs.lib.systems.flakeExposed;
      imports = [
        inputs.haskell-flake.flakeModule
        inputs.pre-commit-hooks-nix.flakeModule
        inputs.flake-parts.flakeModules.easyOverlay
      ];
      perSystem = {
        self',
        system,
        config,
        lib,
        pkgs,
        ...
      }: let
        curl' = pkgs.curl.override {
          c-aresSupport = true;
          brotliSupport = true;
          zstdSupport = true;
        };
      in {
        pre-commit.settings.hooks = {
          alejandra.enable = true;
          deadnix.enable = true;
          statix.enable = true;
          fourmolu.enable = true;
          cabal-fmt.enable = true;
        };
        _module.args.pkgs = import self.inputs.nixpkgs {
          inherit system;
          overlays = [
            self.overlays.default
          ];
        };

        haskellProjects.default = {
          autoWire = [
            "packages"
            "checks"
          ];
          devShell = {
            benchmark = true;
            extraLibraries = hp: {
              inherit (hp) atomic-primops criterion http-client http-conduit;
            };
          };
          settings = {
            hcurl = {self, ...}: {
              benchmark = true;
              extraConfigureFlags = [
                "--ghc-options=-lcurl"
                "--ghc-options=-L${lib.makeLibraryPath [curl']}"
                ''--c2hs-options="--cppopts=--std=gnu18"''
              ];
              custom = prev:
                pkgs.haskell.lib.overrideCabal prev (drv: {
                  libraryToolDepends = (drv.libraryToolDepends or []) ++ [self.c2hs];
                });
            };
          };
        };
        overlayAttrs = {
          uv = pkgs.libuv;
          libcurl = curl';
        };
        packages.default = self'.packages.hcurl;
        devShells.default = pkgs.mkShell {
          inputsFrom = [
            config.pre-commit.devShell
            config.haskellProjects.default.outputs.devShell
          ];
          packages = with pkgs; [
            uv
            libcurl
            libcurl.dev
          ];
          nativeBuildInputs = with pkgs; [
            pkg-config
          ];
          GHC_INCLUDE_LOCATION = pkgs.haskellPackages.ghc;
        };
      };
    };
}
