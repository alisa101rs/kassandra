{
  description = "Kassandra Node package";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    systems.url = "github:nix-systems/default";
    flake-utils.url = "github:numtide/flake-utils";
    flake-parts.url = "github:hercules-ci/flake-parts";
    crane.url = "github:ipetkov/crane";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs:
    inputs.flake-parts.lib.mkFlake {inherit inputs;} {
      debug = true;
      systems = import inputs.systems;
      perSystem = {
        inputs',
        pkgs,
        system,
        ...
      }: {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [
            (final: prev: {
              cassandra = prev.cassandra.override {
                python = prev.python3;
              };
            })
          ];
        };
        packages.default = pkgs.callPackage ./tools/kassandra-node.nix {
          crane = inputs.crane;
          fenix = inputs.fenix.packages.${ system };
        };
        formatter = pkgs.alejandra;

        devShells.default = let
          cassandra = pkgs.callPackage ./tools/cassandra.nix {};
          cqlsh = pkgs.callPackage ./tools/cqlsh.nix {};
        in
          pkgs.mkShell {
            nativeBuildInputs = [
              cassandra
              cqlsh
            ];
          };
      };
    };
}
