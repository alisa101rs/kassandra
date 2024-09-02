{
  crane,
  fenix,
  pkgs,
  lib,
  system,
  ...
}: let
  craneLib =
    (crane.mkLib pkgs).overrideToolchain
    fenix.minimal.toolchain;
in
  craneLib.buildPackage {
    src = ../.;
    cargoExtraArgs = "-p kassandra-node";
    pname = "kassandra-node";
    nativeBuildInputs =
      [
      ]
      ++ lib.optionals pkgs.stdenv.isLinux [
        pkgs.pkg-config
        pkgs.openssl.dev
      ]
      ++ lib.optionals pkgs.stdenv.isDarwin [
        pkgs.darwin.apple_sdk.frameworks.Security
        pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
        pkgs.darwin.apple_sdk.frameworks.CoreServices
        pkgs.libiconv
      ];
  }
