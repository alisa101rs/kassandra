use crate::cql::{
    column::ColumnType,
    schema::{
        keyspace::{Keyspace, Strategy},
        Column, ColumnKind, PrimaryKey, Table, TableSchema,
    },
};

pub fn system_keyspace() -> (String, Keyspace) {
    let keyspace = Keyspace {
        name: "system".to_string(),
        strategy: Strategy::LocalStrategy,
        tables: [peers(), local()].into_iter().collect(),
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
        dse_version: ColumnType::Text,
        graph: ColumnType::Text,
        host_id: ColumnType::Uuid,
        preferred_ip: ColumnType::Inet,
        rack: ColumnType::Text,
        release_version: ColumnType::Text,
        rpc_address: ColumnType::Inet,
        schema_version: ColumnType::Uuid,
        server_id: ColumnType::Text,
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
        cluster_name: ColumnType::Text,
        cql_version: ColumnType::Text,
        data_center: ColumnType::Text,
        dse_version: ColumnType::Text,
        gossip_generation: ColumnType::Int,
        graph: ColumnType::Text,
        host_id: ColumnType::Uuid,
        listen_address: ColumnType::Inet,
        native_protocol_version: ColumnType::Text,
        partitioner: ColumnType::Text,
        rack: ColumnType::Text,
        release_version: ColumnType::Text,
        rpc_address: ColumnType::Inet,
        schema_version: ColumnType::Uuid,
        server_id: ColumnType::Text,
        thrift_version: ColumnType::Text,
        tokens: ColumnType::Set(Box::new(ColumnType::Text)),
        truncated_at: ColumnType::Map(Box::new(ColumnType::Uuid), Box::new(ColumnType::Blob)),
        workload: ColumnType::Text,
        workloads: ColumnType::Text
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
        allow_auto_snapshot: ColumnType::Boolean,
        incremental_backups: ColumnType::Boolean,
        cdc: ColumnType::Boolean
    ]
);

system_table!(
    system_schema.views;
    [keyspace_name: ColumnType::Text],
    [view_name: ColumnType::Text],
    [
        base_table_name: ColumnType::Text
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
