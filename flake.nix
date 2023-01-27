{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane = {
      url = "github:ipetkov/crane";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    sqllogictest.url = "github:wsx-ucb/sqllogictest-rs";
  };

  outputs = { self, nixpkgs, flake-utils, crane, fenix, sqllogictest }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        toolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain;
          sha256 = "sha256-twStjIoEFC9YTbJN49x81IxSeOXrxKm8xEoae9Z1pmM=";
        };
        craneLib = crane.lib.${system}.overrideToolchain toolchain;
        packageDef = with pkgs; {
          src = craneLib.cleanCargoSource ./.;
          nativeBuildInputs = [ lld pkg-config autoPatchelfHook perl cmake ];
          buildInputs = [ openssl cyrus_sasl protobuf curl ];
          runtimeDependencies = with pkgs; [ curl zlib cyrus_sasl stdenv.cc.cc.lib ];
          cargoExtraArgs = "--features \"static-link static-log-level\"";
        };
        cargoArtifacts = craneLib.buildDepsOnly packageDef;
        risingwave = craneLib.buildPackage (packageDef // {
          inherit cargoArtifacts;
        });
      in {
        packages.default = risingwave;

        devShells.default = pkgs.mkShell rec {
          inputsFrom = [ risingwave ];
          packages = with pkgs; [
            rust-analyzer
            cargo-make
            cargo-hakari
            cargo-sort
            cargo-llvm-cov
            cargo-nextest
            cargo-udeps
            typos
            bash
            cacert
            curl
            tmux
            etcd
            postgresql
            sqllogictest.packages.${system}.default
          ];
          CURL_CA_BUNDLE = "/etc/ssl/certs/ca-bundle.crt";
          LD_LIBRARY_PATH = pkgs.lib.strings.makeLibraryPath (builtins.concatMap (d: d.runtimeDependencies) inputsFrom);
        };
      });
}
