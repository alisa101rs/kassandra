use insta::assert_debug_snapshot;
use kassandra::{
    frame::{request::query::Query, response::result::QueryResult},
    KassandraSession,
};

macro_rules! exec {
    ($session: tt, $query: tt) => {
        $session.process(Query::simple($query).unwrap()).unwrap()
    };
}

pub fn session() -> KassandraSession {
    let mut session = KassandraSession::new();
    let result = exec!(
        session,
        "CREATE KEYSPACE cycling
          WITH REPLICATION = {
           'class' : 'SimpleStrategy',
           'replication_factor' : 1
          };"
    );

    assert!(matches! {result, QueryResult::SchemaChange(_)});

    let result = exec!(
        session,
        "CREATE TABLE cycling.cyclist_name (
                       id int PRIMARY KEY,
                       lastname text,
                       firstname text );"
    );
    assert!(matches! {result, QueryResult::SchemaChange(_)});
    session
}

#[test]
fn create_session_and_table() {
    let _ = session();
}

#[test]
fn scan_simple_data() {
    let mut session = session();
    let result = exec!(
        session,
        "insert into cycling.cyclist_name (id, lastname, firstname) values (1, 'john', 'johnson');"
    );
    assert!(matches! { result, QueryResult::Void});
    let QueryResult::Rows(rows) = exec!(session, "select * from cycling.cyclist_name;") else {
        panic!("invalid return type");
    };
    assert_debug_snapshot!("select all", rows);

    let QueryResult::Rows(rows) = exec!(session, "select id, firstname from cycling.cyclist_name;")
    else {
        panic!("invalid return type");
    };
    assert_debug_snapshot!("select 2 columns", rows);

    let QueryResult::Rows(rows) = exec!(session, "select json * from cycling.cyclist_name;") else {
        panic!("invalid return type");
    };
    assert_eq!(rows.metadata.col_count, 1);
    assert_debug_snapshot!("select json", rows);
}
