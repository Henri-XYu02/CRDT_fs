{
  inputs = { utils.url = "github:numtide/flake-utils"; };
  outputs = { self, nixpkgs, utils }:
    utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [ uv python313 ];
          nativeBuildInputs = with pkgs; [ ruff basedpyright ];
          shellHook = ''
            export UV_PYTHON=${pkgs.python313}
            uv venv
            source .venv/bin/activate
          '';
        };
      });
}
