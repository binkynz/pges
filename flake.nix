{
  description = "pges development shell: Rust toolchain and a local PostgreSQL 18 for tests";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      devShells = forAll (pkgs:
        let
          pg = pkgs.postgresql_18;
          port = "54318";

          # Starts a throwaway cluster in ./.pg on a unix socket. Same settings as the test container
          # (crates/pges-testing/src/lib.rs, SERVER_ARGS).
          pg-up = pkgs.writeShellApplication {
            name = "pg-up";
            runtimeInputs = [ pg ];
            text = ''
              dir="''${PGES_PG_DIR:-$PWD/.pg}"
              if [ ! -d "$dir/data" ]; then
                mkdir -p "$dir"
                initdb -D "$dir/data" -U postgres --auth=trust --no-locale -E UTF8 >/dev/null
              fi
              if pg_ctl -D "$dir/data" status >/dev/null 2>&1; then
                echo "PostgreSQL already running in $dir"
                exit 0
              fi
              pg_ctl -D "$dir/data" -l "$dir/log" -w start -o "\
                -k $dir -c listen_addresses= -p ${port} \
                -c wal_level=logical -c max_replication_slots=64 -c max_wal_senders=64 \
                -c max_connections=300 -c fsync=off -c synchronous_commit=off -c full_page_writes=off"
              echo "PostgreSQL ${pg.version} running in $dir"
            '';
          };

          # Stops the cluster. --clean also deletes it, including databases left by failed tests.
          pg-down = pkgs.writeShellApplication {
            name = "pg-down";
            runtimeInputs = [ pg ];
            text = ''
              dir="''${PGES_PG_DIR:-$PWD/.pg}"
              if [ -d "$dir/data" ]; then pg_ctl -D "$dir/data" -m fast stop || true; fi
              if [ "''${1:-}" = "--clean" ]; then rm -rf "$dir"; fi
            '';
          };
        in
        {
          default = pkgs.mkShell {
            packages = [
              pg pg-up pg-down
              pkgs.cargo pkgs.rustc pkgs.clippy pkgs.rustfmt pkgs.rust-analyzer
              pkgs.cargo-nextest pkgs.cargo-deny
            ];
            shellHook = ''
              # pges-testing finds the pg-up cluster by its socket in ./.pg; nothing to export.
            '';
          };
        });
    };
}
