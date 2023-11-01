use crate::cql::{
    column::ColumnType,
    schema::{
        keyspace::{Keyspace, Strategy},
        Column, ColumnKind,
        ColumnType::*,
        PrimaryKey, Table, TableSchema,
    },
};

pub fn system_keyspace() -> (String, Keyspace) {
    let keyspace = Keyspace {
        name: "system".to_string(),
        strategy: Strategy::LocalStrategy,
        tables: [
            local(),
            available_ranges(),
            available_ranges_v2(),
            batches(),
            build_views(),
            compaction_history(),
            indexinfo(),
            paxos(),
            peer_events(),
            peer_events_v2(),
            peers(),
            peers_v2(),
            prepared_statements(),
            repairs(),
            size_estimates(),
            sstable_activity(),
            table_estimates(),
            transferred_ranges(),
            transferred_ranges_v2(),
            view_builds_in_progress(),
        ]
        .into_iter()
        .collect(),
        user_defined_types: Default::default(),
    };

    ("system".to_string(), keyspace)
}

pub fn system_schema_keyspace() -> (String, Keyspace) {
    (
        "system_schema".to_string(),
        Keyspace {
            name: "system_schema".to_string(),
            strategy: Strategy::LocalStrategy,
            tables: [
                types(),
                columns(),
                tables(),
                views(),
                keyspaces(),
                indexes(),
                functions(),
                aggregates(),
                triggers(),
            ]
            .into_iter()
            .collect(),
            user_defined_types: Default::default(),
        },
    )
}

macro_rules! system_table {
    (
        $keyspace:ident . $table:ident;
        [$( $pk_name:ident: $pk_type: expr ),+],
        [$( $clustering_name:ident: $clustering_type: expr ),*],
        [$( $column_name:ident: $column_type: expr ),+ ]
    ) => {
        fn $table() -> (String, Table) {
            let columns = [
                $( (stringify!($pk_name).to_string(), Column{ ty: $pk_type, kind:  ColumnKind::PartitionKey }), )*
                $( (stringify!($clustering_name).to_string(), Column{ ty: $clustering_type, kind:  ColumnKind::Clustering }), )*
                $( (stringify!($column_name).to_string(), Column{ ty: $column_type, kind:  ColumnKind::Regular }), )*
            ].into_iter().collect();

            let schema = TableSchema {
                columns,
                partition_key: PrimaryKey::from_definition([
                    $( stringify!($pk_name).to_string(), )*
                ].into_iter().collect()),
                clustering_key: PrimaryKey::from_definition([
                    $( stringify!($clustering_name).to_string(), )*
                ].into_iter().collect()),
                partitioner: None,
            };

            let table = Table {
                keyspace: stringify!($keyspace).to_string(),
                name: stringify!($table).to_string(),
                schema,
            };

            (stringify!($table).to_string(), table)
        }
    };
}

system_table! {
    system.peers;
    [peer: ColumnType::Inet],
    [],
    [
        data_center: ColumnType::Text,
        host_id: ColumnType::Uuid,
        preferred_ip: ColumnType::Inet,
        rack: ColumnType::Text,
        release_version: ColumnType::Text,
        rpc_address: ColumnType::Inet,
        schema_version: ColumnType::Uuid,
        tokens: ColumnType::Set(Box::new(ColumnType::Text))
    ]
}

system_table! {
    system.peers_v2;
    [peer: ColumnType::Inet],
    [peer_port: ColumnType::Int],
    [
        data_center: ColumnType::Text,
        host_id: ColumnType::Uuid,
        preferred_ip: ColumnType::Inet,
        preferred_port: ColumnType::Int,
        rack: ColumnType::Text,
        release_version: ColumnType::Text,
        native_address: ColumnType::Inet,
        native_port: ColumnType::Int,
        schema_version: ColumnType::Uuid,
        tokens: ColumnType::Set(Box::new(ColumnType::Text))
    ]
}

system_table!(
    system.local;
    [key: ColumnType::Text],
    [],
    [
        bootstrapped: ColumnType::Text,
        broadcast_address: ColumnType::Inet,
        broadcast_port: ColumnType::Int,
        cluster_name: ColumnType::Text,
        cql_version: ColumnType::Text,
        data_center: ColumnType::Text,
        gossip_generation: ColumnType::Int,
        host_id: ColumnType::Uuid,
        listen_address: ColumnType::Inet,
        listen_port: ColumnType::Int,
        native_protocol_version: ColumnType::Text,
        partitioner: ColumnType::Text,
        rack: ColumnType::Text,
        release_version: ColumnType::Text,
        rpc_address: ColumnType::Inet,
        rpc_port: ColumnType::Int,
        schema_version: ColumnType::Uuid,
        tokens: ColumnType::Set(Box::new(ColumnType::Text)),
        truncated_at: ColumnType::Map(Box::new(ColumnType::Uuid), Box::new(ColumnType::Blob))
    ]
);

system_table!(
    system.available_ranges;
    [keyspace_name: ColumnType::Text],
    [],
    [ranges: ColumnType::Set(Box::new(ColumnType::Blob))]
);

system_table!(
    system.available_ranges_v2;
    [keyspace_name: ColumnType::Text],
    [],
    [
        full_ranges: ColumnType::Set(Box::new(ColumnType::Blob)),
        transient_ranges: ColumnType::Set(Box::new(ColumnType::Blob))
    ]
);

system_table!(
    system.batches;
    [id: ColumnType::Timeuuid],
    [],
    [
        mutations: ColumnType::List(Box::new(ColumnType::Blob)),
        version: ColumnType::Int
    ]
);

system_table!(
    system.build_views;
    [keyspace_name: ColumnType::Text],
    [view_name: ColumnType::Text],
    [
      status_replicated: ColumnType::Boolean
    ]
);

system_table!(
    system.compaction_history;
    [id: ColumnType::Uuid],
    [],
    [
        bytes_in: ColumnType::BigInt,
        bytes_out: ColumnType::BigInt,
        columnfamily_name: ColumnType::Text,
        compacted_at: ColumnType::Timeuuid
    ]
);

system_table!(
    system.indexinfo;
    [table_name: ColumnType::Text],
    [index_name: ColumnType::Text],
    [
        value: ColumnType::Blob
    ]
);

system_table!(
    system.paxos;
    [row_key: ColumnType::Blob],
    [cf_id: ColumnType::Uuid],
    [
        in_progress_ballot: ColumnType::Timeuuid,
        most_recent_commit: ColumnType::Blob,
        most_recent_commit_at: Timeuuid,
        most_recent_commit_version: Int,
        proposal: Blob,
        proposal_ballot: Timeuuid,
        proposal_version: Int
    ]
);

system_table!(
    system.peer_events;
    [peer: Inet],
    [],
    [hints_dropped: Map(Box::new(Uuid), Box::new(Int))]
);

system_table!(
    system.peer_events_v2;
    [peer: Inet],
    [peer_port: Int],
    [hints_dropped: Map(Box::new(Uuid), Box::new(Int))]
);

system_table!(
    system.prepared_statements;
    [prepared_id: Blob],
    [],
    [
        logged_keyspace: Text,
        query_string: Text
    ]
);

system_table!(
    system.repairs;
    [parent_id: Timeuuid],
    [],
    [
       cfids: Set(Box::new(Uuid)),
        coordinator: Inet,
        coordinator_port: Int,
        last_update: Timestamp,
        participants: Set(Box::new(Inet)),
        participants_wp: Set(Box::new(Text)),
        ranges: Set(Box::new(Blob)),
        repaired_at: Timestamp,
        started_at: Timestamp,
        state: Int
    ]
);

system_table!(
    system.size_estimates;
    [keyspace_name: Text],
    [
        table_name: Text,range_start: Text,
        range_end: Text
    ],
    [
        mean_partition_size: BigInt,
        partitions_count: BigInt
    ]
);

system_table!(
    system.sstable_activity;
    [keyspace_name: Text],
    [
        columnfamily_name: Text,
        generation: Int
    ],
    [
        rate_120m: Double,
        rate_15m: Double
    ]
);

system_table!(
    system.table_estimates;
    [keyspace_name: Text],
    [
        table_name: Text,
        range_type: Text,
        range_start: Text,
        range_end: Text
    ],
    [
        mean_partition_size: BigInt,
        partitions_count: BigInt
    ]
);

system_table!(
    system.transferred_ranges;
    [
        operation: Text,
        keyspace_name: Text
    ],
    [
        peer: Inet
    ],
    [
        ranges: Set(Box::new(Blob))
    ]
);

system_table!(
    system.transferred_ranges_v2;
    [
        operation: Text,
        keyspace_name: Text
    ],
    [
        peer: Inet,
        peer_port: Int
    ],
    [
        ranges: Set(Box::new(Blob))
    ]
);

system_table!(
    system.view_builds_in_progress;
    [
        keyspace_name: Text
    ],
    [
        view_name: Text,
        start_token: Text,
        end_token: Text
    ],
    [
        keys_build: BigInt,
        last_token: Text
    ]
);

system_table!(
    system_schema.types;
    [keyspace_name: ColumnType::Text],
    [type_name: ColumnType::Text],
    [
        field_names: ColumnType::List(Box::new(ColumnType::Text)),
        field_types: ColumnType::List(Box::new(ColumnType::Text))
    ]
);

system_table!(
    system_schema.columns;
    [keyspace_name: ColumnType::Text],
    [
        table_name: ColumnType::Text,
        column_name: ColumnType::Text
    ],
    [
        clustering_order: ColumnType::Text,
        column_name_bytes: ColumnType::Blob,
        kind: ColumnType::Text,
        position: ColumnType::Int,
        type: ColumnType::Text
    ]
);

system_table!(
    system_schema.tables;
    [keyspace_name: ColumnType::Text],
    [table_name: ColumnType::Text],
    [
        additional_write_policy: ColumnType::Text,
        bloom_filter_fp_chance: ColumnType::Double,
        caching: Map(Box::new(Text), Box::new(Text)),
        cdc: ColumnType::Boolean,
        comment: Text,
        compaction: Map(Box::new(Text), Box::new(Text)),
        compression: Map(Box::new(Text), Box::new(Text)),
        crc_check_chance: Double,
        dclocal_read_repair_chance: Double,
        default_time_to_live: Int,
        extensions: Map(Box::new(Text), Box::new(Text)),
        flags: Set(Box::new(Text)),
        gc_grace_seconds: Int,
        id: Uuid,
        max_index_interval: Int,
        memtable: Text,
        memtable_flush_period_in_ms: Int,
        min_index_interval: Int,
        read_repair: Text,
        read_repair_chance: Double,
        speculative_retry: Text
    ]
);

system_table!(
    system_schema.views;
    [keyspace_name: ColumnType::Text],
    [view_name: ColumnType::Text],
    [
        additional_write_policy: ColumnType::Text,
        base_table_id: Uuid,
        base_table_name: Text,
        bloom_filter_fp_chance: ColumnType::Double,
        caching: Map(Box::new(Text), Box::new(Text)),
        cdc: ColumnType::Boolean,
        comment: Text,
        compaction: Map(Box::new(Text), Box::new(Text)),
        compression: Map(Box::new(Text), Box::new(Text)),
        crc_check_chance: Double,
        dclocal_read_repair_chance: Double,
        default_time_to_live: Int,
        extensions: Map(Box::new(Text), Box::new(Text)),
        gc_grace_seconds: Int,
        id: Uuid,
        include_all_columns: Boolean,
        max_index_interval: Int,
        memtable: Text,
        memtable_flush_period_in_ms: Int,
        min_index_interval: Int,
        read_repair: Text,
        read_repair_chance: Double,
        speculative_retry: Text,
        where_clause: Text
    ]
);

system_table!(
    system_schema.keyspaces;
    [keyspace_name: ColumnType::Text],
    [],
    [
        durable_writes: ColumnType::Boolean,
        replication: ColumnType::Map(Box::new(ColumnType::Text), Box::new(ColumnType::Text))
    ]
);

system_table!(
    system_schema.indexes;
    [keyspace_name: ColumnType::Text],
    [
        table_name: ColumnType::Text,
        index_name: ColumnType::Text
    ],
    [
        kind: ColumnType::Text,
        options: ColumnType::Map(Box::new(ColumnType::Text), Box::new(ColumnType::Text))
    ]
);

system_table!(
    system_schema.functions;
    [keyspace_name: ColumnType::Text],
    [
        function_name: ColumnType::Text,
        argument_types: ColumnType::List(Box::new(ColumnType::Text))
    ],
    [
        argument_names: ColumnType::List(Box::new(ColumnType::Text)),
        body: ColumnType::Text,
        language: ColumnType::Text,
        return_type: ColumnType::Text,
        called_on_null_input: ColumnType::Boolean
    ]
);

system_table!(
    system_schema.aggregates;
    [keyspace_name: ColumnType::Text],
    [
        aggregate_name: ColumnType::Text,
        argument_types: ColumnType::List(Box::new(ColumnType::Text))
    ],
    [
        final_func: ColumnType::Text,
        initcond: ColumnType::Text,
        return_type: ColumnType::Text,
        state_func: ColumnType::Text,
        state_type: ColumnType::Text
    ]
);

system_table!(
    system_schema.triggers;
    [keyspace_name: ColumnType::Text],
    [
        table_name: ColumnType::Text,
        trigger_name: ColumnType::Text
    ],
    [
        options: ColumnType::Map(Box::new(ColumnType::Text), Box::new(ColumnType::Text))
    ]
);
