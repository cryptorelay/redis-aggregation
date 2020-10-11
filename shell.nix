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
      redis
      (import ./integration_tests { inherit pkgs; })
    ];

    shellHook = ''
      export AGGREGATION_LIBRARY=${aggLib}
    '';
  }
