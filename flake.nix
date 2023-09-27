{
    description = "Kassandra Node package";
    inputs = {
         nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
         fenix = {
           url = "github:nix-community/fenix";
           inputs.nixpkgs.follows = "nixpkgs";
         };
         flake-utils.url = "github:numtide/flake-utils";
   };

    outputs = {
        self,
        nixpkgs,
        fenix,
        flake-utils,
    }: flake-utils.lib.eachDefaultSystem (system: {
         packages.default =
           let
             toolchain = fenix.packages.${system}.minimal.toolchain;
             pkgs = nixpkgs.legacyPackages.${system};
           in

           (pkgs.makeRustPlatform {
             cargo = toolchain;
             rustc = toolchain;
           }).buildRustPackage {
             pname = "kassandra-node";
             version = "0.10.0";

             src = ./.;
             nativeBuildInputs = [
                pkgs.protobuf
             ];
             cargoLock.lockFile = ./Cargo.lock;
           };
       });
}
