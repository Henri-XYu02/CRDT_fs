{
  inputs = { utils.url = "github:numtide/flake-utils"; };
  outputs = { self, nixpkgs, utils }:
    utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [ uv python313 pkg-config fuse3 stdenv.cc.cc stdenv.cc.cc.lib glib.out libGL cacert openssl ];
          nativeBuildInputs = with pkgs; [ ruff basedpyright ];
          shellHook = ''
            export UV_PYTHON=${pkgs.python313}
            uv venv
            source .venv/bin/activate
            export LD_LIBRARY_PATH=${
              pkgs.lib.makeLibraryPath [
                pkgs.stdenv.cc.cc
                pkgs.stdenv.cc.cc.lib
                pkgs.glib.out
                pkgs.libGL
              ]
            }
          '';
        };
      });
}
