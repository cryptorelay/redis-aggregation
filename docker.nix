{ system ? "x86_64-linux", pkgs ? import <nixpkgs> { inherit system; } }:
with pkgs;
let
  agg = import ./. { inherit pkgs; };
  aggLib = agg + "/lib/libredis_aggregation." + (
    if stdenv.isDarwin then "dylib" else "so"
  );
in
dockerTools.buildLayeredImage {
  name = "cryptorelay/redis-aggregation";
  config.Entrypoint = [
    "${redis}/bin/redis-server"
    "--loadmodule"
    "${aggLib}"
  ];
}
