{
  cassandra,
  lib,
  pkgs,
  ...
}: let
  yamlFormat = pkgs.formats.yaml {};
  defExtraConfig = {
    start_native_transport = "true";
    listen_address = "127.0.0.1";
    native_transport_port = "9042";
    commitlog_sync = "batch";
    commitlog_sync_batch_window_in_ms = 2;
    cluster_name = "cluster1";
    partitioner = "org.apache.cassandra.dht.Murmur3Partitioner";
    endpoint_snitch = "SimpleSnitch";
    data_file_directories = ["./data"];
    commitlog_directory = "./data/commitlog";
    saved_caches_directory = "./data/saved_caches";
    hints_directory = "./data/hints";
    seed_provider = [
      {
        class_name = "org.apache.cassandra.locator.SimpleSeedProvider";
        parameters = [{seeds = lib.concatStringsSep "," ["127.0.0.1"];}];
      }
    ];
  };

  cassandraConfig = pkgs.stdenv.mkDerivation {
    name = "cassandra-config";
    cassandraYaml = yamlFormat.generate "cassandra.yaml" defExtraConfig;
    buildCommand = ''
      mkdir -p $out
      for d in ${cassandra}/conf/*; do ln -s "$d" $out/; done
      rm -rf $out/cassandra.y*ml
      ln -s "$cassandraYaml" "$out/cassandra.yaml"

      rm -rf $out/cassandra-env.sh
      cat ${cassandra}/conf/cassandra-env.sh > $out/cassandra-env.sh
      LOCAL_JVM_OPTS=""
      echo "JVM_OPTS=\"\$JVM_OPTS $LOCAL_JVM_OPTS\"" >> $out/cassandra-env.sh
    '';
  };
  dataDir = "./data";
in
  pkgs.writeShellApplication {
    name = "start-cassandra";
    runtimeInputs = [pkgs.coreutils cassandra];
    text = ''
      set -euo pipefail

      DATA_DIR="$(readlink -m ${dataDir})"
      if [[ ! -d "$DATA_DIR" ]]; then
      mkdir -p "$DATA_DIR"
      fi

      CASSANDRA_CONF="${cassandraConfig}"
      export CASSANDRA_CONF

      CASSANDRA_LOG_DIR="$DATA_DIR/log/"
      mkdir -p "$CASSANDRA_LOG_DIR"
      export CASSANDRA_LOG_DIR

      CASSANDRA_HOME="${cassandra}"
      export CASSANDRA_HOME

      CLASSPATH="${cassandra}/lib"
      export CLASSPATH

      export LOCAL_JMX="yes"
      exec cassandra -f
    '';
  }
