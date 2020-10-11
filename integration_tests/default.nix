{ pkgs ? import <nixpkgs> {} }:
with pkgs;

poetry2nix.mkPoetryEnv {
  projectDir = ../.;
  overrides = poetry2nix.overrides.withDefaults (self: super: {
    credis = super.credis.overridePythonAttrs (
      old: {
        buildInputs = old.buildInputs ++ [self.cython];
      }
    );
  });
}
