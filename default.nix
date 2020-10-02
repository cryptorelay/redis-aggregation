{ pkgs ? import <nixpkgs> {} }:
with pkgs;
rustPlatform.buildRustPackage {
  pname = "redis-aggregation";
  version = "0.0.1";
  src = lib.cleanSourceWith {
    name = "src";
    src = lib.sourceByRegex ./. [
      "^Cargo.toml$"
      "^Cargo.lock$"
      "^src$"
      "^src/.*"
      "^redismodule-rs$"
      "^redismodule-rs/.*"
    ];
  };
  cargoSha256 = sha256:1rcxkhazhfbswqq99n9a68xvhl7ywk1n3gqi412bv89ikyl3jfzr;
}
