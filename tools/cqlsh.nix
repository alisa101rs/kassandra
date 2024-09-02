{
  stdenv,
  python3,
  makeWrapper,
  fetchurl,
  ...
}: let
  version = "4.1.6";
in
  stdenv.mkDerivation {
    name = "cqlsh";
    pname = "cqlsh";
    src = fetchurl {
      sha256 = "2f51ce787812cce2ffb3db83a9a23248537fb52123884b0855551a0555ae8d03";
      url = "https://dlcdn.apache.org/cassandra/${version}/apache-cassandra-${version}-bin.tar.gz";
    };
    nativeBuildInputs = [makeWrapper];
    installPhase = ''
      runHook preInstall

      mkdir -p $out/bin
      mkdir -p $out/lib

      mv pylib $out/pylib
      mv lib/cassandra-driver* $out/lib
      mv lib/six* $out/lib
      mv lib/pure_sasl* $out/lib
      mv ./bin/cqlsh $out/bin/cqlsh
      mv ./bin/cqlsh.py $out/bin/cqlsh.py

      wrapProgram $out/bin/cqlsh --prefix PATH : ${python3}/bin

      runHook postInstall
    '';
  }
