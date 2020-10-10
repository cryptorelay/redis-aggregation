{ system ? "x86_64-linux", pkgs ? import <nixpkgs> { inherit system; } }:
with pkgs;
let
  module = import ./. { inherit pkgs; };
in
dockerTools.buildLayeredImage {
  name = "cryptorelay/redis-aggregation";
  config.Entrypoint = [
    "${redis}/bin/redis-server"
    "--loadmodule"
    "${module}/lib/libredis_aggregation.so"
  ];
}
