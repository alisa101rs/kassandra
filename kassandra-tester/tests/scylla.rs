use std::collections::HashSet;

use insta::assert_yaml_snapshot;
use kassandra::kassandra::Kassandra;
use kassandra_tester::KassandraTester;
use scylla::{
    batch::{Batch, BatchType},
    FromRow, SessionBuilder, ValueList,
};

#[derive(Debug, ValueList, FromRow, Eq, PartialEq, Hash)]
struct TestData {
    key: String,
    c1: String,
    c2: String,
    value: String,
}

#[tokio::test]
async fn test_simple_test_data() -> eyre::Result<()> {
    let kassandra = Kassandra::new();

    let kassandra = KassandraTester::new(kassandra)
        .in_scope(|addr| async move {
            let s = SessionBuilder::new()
                .known_node(format!("{addr}"))
                .build()
                .await?;

            s
                .query("create keyspace if not exists test123 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", ())
                .await.unwrap();

            s.query(
                "create table if not exists test123.test (key text, c1 text, c2 text, value text, PRIMARY KEY ((key), c1, c2))",
                (),
            )
                .await
                .unwrap();



            let values = [
                TestData {
                    key: "key".to_string(),
                    c1: "c1".to_string(),
                    c2: "c2".to_string(),
                    value: "value".to_string(),
                },
                TestData {
                    key: "key".to_string(),
                    c1: "c1".to_string(),
                    c2: "c22".to_string(),
                    value: "value".to_string(),
                },
                TestData {
                    key: "key".to_string(),
                    c1: "c12".to_string(),
                    c2: "c2".to_string(),
                    value: "value".to_string(),
                },
                TestData {
                    key: "key2".to_string(),
                    c1: "c1".to_string(),
                    c2: "c2".to_string(),
                    value: "value".to_string(),
                },
            ];

            for v in &values {
                s.query(
                    "insert into test123.test (key, c1, c2, value) values(?, ?, ?, ?)",
                    v,
                )
                    .await
                    .unwrap();
            }

            let r = s
                .query("select key, c1, c2, value from test123.test", &())
                .await
                .unwrap()
                .rows_typed::<TestData>()
                .unwrap();

            let rows = r.into_iter().collect::<Result<HashSet<_>, _>>()
                .unwrap();

            assert!(
                values.iter().all(|it| rows.contains(it))
            );

            let r = s
                .query(
                    "select key, c1, c2, value from test123.test where key=?",
                    &("key",),
                )
                .await
                .unwrap()
                .rows_typed::<TestData>()
                .unwrap();

            for r in r {
                let r = r.unwrap();
                assert_eq!(&r.key, "key")
            }

            let r = s
                .query(
                    "select key, c1, c2, value from test123.test where key=? AND c1 = ?",
                    &("key", "c1"),
                )
                .await
                .unwrap()
                .rows_typed::<TestData>()
                .unwrap();

            for r in r {
                let r = r.unwrap();
                assert_eq!(&r.key, "key");
                assert_eq!(&r.c1, "c1");
            }

            let r = s
                .query(
                    "select key, c1, c2, value from test123.test where key=? AND c1 = ? AND c2 = ?",
                    &("key", "c1", "c22"),
                )
                .await
                .unwrap()
                .maybe_first_row_typed::<TestData>()
                .unwrap();

            let r = r.unwrap();

            assert_eq!(&r.key, "key");
            assert_eq!(&r.c2, "c22");

            Ok::<(), eyre::Report>(())
        }).await?;

    assert_yaml_snapshot!(kassandra.data_snapshot());

    Ok(())
}

#[derive(Debug, ValueList, FromRow, Eq, PartialEq, Hash)]
struct TestBatchData {
    key: String,
    value: String,
}
#[tokio::test]
async fn test_simple_batch_data() -> eyre::Result<()> {
    let kassandra = Kassandra::new();

    let test = |addr| async move {
        let s = SessionBuilder::new()
            .known_node(format!("{addr}"))
            .build()
            .await?;

        s
            .query("create keyspace if not exists test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", ())
            .await.unwrap();

        s.query(
            "create table if not exists test.t1 (key text, value text, PRIMARY KEY ((key)))",
            (),
        )
        .await
        .unwrap();

        s.query(
            "create table if not exists test.t2 (key text, value text, PRIMARY KEY ((key)))",
            (),
        )
        .await
        .unwrap();

        let mut batch = Batch::new(BatchType::Unlogged);
        batch.append_statement("insert into test.t1 (key, value) values(?, ?)");
        batch.append_statement("insert into test.t2 (key, value) values(?, ?)");
        let values = (
            TestBatchData {
                key: "key".to_string(),
                value: "value".to_string(),
            },
            TestBatchData {
                key: "key".to_string(),
                value: "value".to_string(),
            },
        );
        s.batch(&batch, values).await.unwrap();

        Ok::<_, eyre::Report>(())
    };
    let kassandra = KassandraTester::new(kassandra).in_scope(test).await?;

    assert_yaml_snapshot!(kassandra.data_snapshot());

    Ok(())
}

#[tokio::test]
async fn test_prepared() -> eyre::Result<()> {
    let kassandra = Kassandra::new();

    let test = |addr| async move {
        let s = SessionBuilder::new()
            .known_node(format!("{addr}"))
            .build()
            .await?;

        s
            .query("create keyspace if not exists test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", ())
            .await.unwrap();

        s.query(
            "create table if not exists test.t1 (key text, value text, PRIMARY KEY ((key)))",
            (),
        )
        .await
        .unwrap();

        let insert = s
            .prepare("insert into test.t1 (key, value) values(?, ?)")
            .await
            .unwrap();
        let select = s
            .prepare("select key, value from test.t1 where key=?")
            .await
            .unwrap();

        let t = TestBatchData {
            key: "key".to_string(),
            value: "value".to_string(),
        };

        s.execute(&insert, t).await.unwrap();

        let v = s
            .execute(&select, ("key",))
            .await
            .unwrap()
            .maybe_first_row_typed::<TestBatchData>()
            .unwrap()
            .unwrap();
        assert_eq!(v.value, "value");

        Ok::<_, eyre::Report>(())
    };

    let kassandra = KassandraTester::new(kassandra).in_scope(test).await?;

    assert_yaml_snapshot!(kassandra.data_snapshot());

    Ok(())
}
