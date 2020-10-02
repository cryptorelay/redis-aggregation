{ pkgs? import <nixpkgs> {} }:
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
  cargoSha256 = sha256:00fzh8jfay0ys4ag648knaxg0qp8qpyw1l8mcv6jp8bk36r7wd34;
}
