{
    description = "Kassandra Node package";
    inputs = {
         nixpkgs.url = "github:NixOS/nixpkgs";
   };

    outputs = {
        self,
        nixpkgs,
    }: let
       allSystems = [
           "x86_64-linux"
            "aarch64-linux"
            "x86_64-darwin"
            "aarch64-darwin"
       ];

       forAllSystems = f: nixpkgs.lib.genAttrs allSystems (system: f {
            pkgs = import nixpkgs { inherit system; };
       });

   in {
        packages = forAllSystems ({ pkgs }: {
            default = pkgs.rustPlatform.buildRustPackage rec {
                pname = "kassandra-node";
                version = "0.7.1";
                cargoLock = {
                    lockFile = ./Cargo.lock;
                };
                nativeBuildInputs = [
                    pkgs.protobuf
                ];
                src = ./.;
                meta = {
                    description = "Kassandra Node";
                };
            };
        });
    };
}
