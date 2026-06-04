{
  inputs = {
    # Common is used only to get the GHC 9.2 package set.
    common.url = "github:nammayatri/common";

    nixpkgs.follows = "common/nixpkgs";
    flake-parts.follows = "common/flake-parts";
    haskell-flake.follows = "common/haskell-flake";

    # Formatting / pre-commit (same toolchain as nammayatri/Backend).
    flake-root.follows = "common/flake-root";
    treefmt-nix.follows = "common/treefmt-nix";
    pre-commit-hooks-nix.follows = "common/pre-commit-hooks-nix";
    nixpkgs-21_11.url = "github:nixos/nixpkgs/nixos-21.11";
    nixpkgs-140774-workaround.url = "github:srid/nixpkgs-140774-workaround";

    cereal.url = "github:juspay/cereal";
    cereal.flake = false;

    juspay-extra.url = "github:juspay/euler-haskell-common";
    juspay-extra.inputs.haskell-flake.follows = "haskell-flake";

    euler-events-hs.url = "github:juspay/euler-events-hs/main";
    euler-events-hs.inputs.haskell-flake.follows = "haskell-flake";

    sequelize.url = "github:nammayatri/haskell-sequelize/backend/decoupled-drainer-changes";
    sequelize.inputs.nixpkgs.follows = "nixpkgs";
    sequelize.inputs.haskell-flake.follows = "haskell-flake";
    sequelize.inputs.flake-parts.follows = "flake-parts";

    hedis.url = "github:nammayatri/hedis/enh/zone-aware-replica-usage";
    hedis.flake = false;

    servant-mock.url = "github:arjunkathuria/servant-mock?rev=17e90cb831820a30b3215d4f164cf8268607891e";
    servant-mock.flake = false;

    tinylog.url = "gitlab:arjunkathuria/tinylog/08d3b6066cd2f883e183b7cd01809d1711092d33";
    tinylog.flake = false;
  };

  outputs = inputs@{ nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = nixpkgs.lib.systems.flakeExposed;
      imports = [
        inputs.common.flakeModules.ghc927
        inputs.haskell-flake.flakeModule
        inputs.flake-root.flakeModule
        inputs.treefmt-nix.flakeModule
        inputs.pre-commit-hooks-nix.flakeModule
      ];
      perSystem = { self', pkgs, lib, config, ... }: {
        # treefmt + pre-commit, mirroring nammayatri/Backend.
        # Run on demand inside `nix develop` with `treefmt`; the pre-commit
        # hook enforces it on every commit.
        treefmt.config = {
          inherit (config.flake-root) projectRootFile;
          flakeCheck = false; # pre-commit-hooks.nix runs the check
          programs.nixpkgs-fmt.enable = true;
          programs.ormolu.enable = true;
          programs.ormolu.package =
            let pkgs-21_11 = import inputs.nixpkgs-21_11 { inherit (pkgs) system; };
            in inputs.nixpkgs-140774-workaround.patch pkgs-21_11 pkgs-21_11.haskellPackages.ormolu;
          settings.formatter.ormolu = {
            options = [
              "--ghc-opt"
              "-XTypeApplications"
              "--ghc-opt"
              "-fplugin=RecordDotPreprocessor"
            ];
            excludes = [
              "dist-newstyle/**"
              "test/jsonb-live/Main.hs"
              # ormolu 0.1.4.1 chokes on these (Haddock-mode parser
              # bugs + idempotency bug on record-update syntax). Same
              # pattern as nammayatri/Backend excluding `vira.hs`.
              "src/EulerHS/Framework/Interpreter.hs"
              "src/EulerHS/HttpAPI.hs"
              "test/language/Common.hs"
              "test/language/KV/TestSchema/ServiceConfiguration.hs"
              "test/db/SQLDB/TestData/Scenarios/SQLite.hs"
            ];
          };
        };

        pre-commit.settings.hooks.treefmt.enable = lib.mkForce true;

        packages.default = self'.packages.euler-hs;
        haskellProjects.default = {
          devShell.tools = _: lib.mkForce ({
            haskell-language-server = null;
            ormolu = pkgs.haskellPackages.ormolu;
            treefmt = config.treefmt.build.wrapper;
          } // config.treefmt.build.programs);
          # Auto-install .git/hooks/pre-commit when entering `nix develop`.
          # `config.pre-commit.installationScript` is provided by
          # pre-commit-hooks-nix.flakeModule.
          devShell.mkShellArgs.shellHook = config.pre-commit.installationScript;
          projectFlakeName = "euler-hs";
          imports = [
            inputs.euler-events-hs.haskellFlakeProjectModules.output
            inputs.juspay-extra.haskellFlakeProjectModules.output
            inputs.sequelize.haskellFlakeProjectModules.output
          ];
          basePackages = config.haskellProjects.ghc927.outputs.finalPackages;
          packages = {
            hedis.source = inputs.hedis;
            cereal.source = inputs.cereal;
            servant-mock.source = inputs.servant-mock;
            tinylog.source = inputs.tinylog;
          };
          settings = {
            bytestring-conversion.broken = false;
            hedis.check = false;
            sequelize.check = false;
            # The euler-hs test-suites (language/db/extra, gated behind the
            # `enable-tests` cabal flag) pull in a few deps that are not part of
            # the library's own dependency closure. Force them into the dev
            # shell so `cabal build --enable-tests -f enable-tests` resolves
            # against the nix-provided (jailbroken) packages instead of falling
            # back to Hackage (which hits a servant-mock base<4.14 conflict).
            euler-hs = { self, ... }: {
              extraBuildDepends = [
                self.servant-mock
                self.generic-arbitrary
                self.quickcheck-instances
              ];
            };
            servant-client = {
              jailbreak = true;
            };
            servant-client-core = {
              jailbreak = true;
            };
            servant-server = {
              jailbreak = true;
            };
            servant-mock = {
              check = false;
              jailbreak = true;
            };
            servant = {
              jailbreak = true;
            };
          };
          autoWire = [ "packages" "checks" "devShells" "apps" ];
        };
      };
    };
}
