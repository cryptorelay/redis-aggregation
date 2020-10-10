{ pkgs ? import <nixpkgs> {} }:
with pkgs;
let
  agg = import ./. { inherit pkgs; };
  aggLib = agg + "/lib/libredis_aggregation." + (
    if stdenv.isDarwin then "dylib" else "so"
  );
in
  mkShell {
    buildInputs = [
      agg
      poetry
      (poetry2nix.mkPoetryEnv {
        projectDir = ./.;
        editablePackageSources = {
          integration_tests = ./integration_tests;
        };
      })
    ];

    shellHook = ''
      export AGGREGATION_LIBRARY=${aggLib}
    '';
  }
