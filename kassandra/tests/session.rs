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
                       firstname text,
                       records map<text, text>);"
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
        "insert into cycling.cyclist_name (id, lastname, firstname, records) values (1, 'john', 'johnson', {'f1': '120', 'f2': '126'});"
    );
    assert!(matches! { result, QueryResult::Void});
    let QueryResult::Rows(rows) = exec!(session, "select * from cycling.cyclist_name;") else {
        panic!("invalid return type");
    };
    assert_debug_snapshot!("select all", rows);

    let QueryResult::Rows(rows) = exec!(
        session,
        "select id, firstname as name, toJson(records) as rec from cycling.cyclist_name;"
    ) else {
        panic!("invalid return type");
    };
    assert_debug_snapshot!("select explicit columns", rows);

    let QueryResult::Rows(rows) = exec!(session, "select json * from cycling.cyclist_name;") else {
        panic!("invalid return type");
    };
    assert_eq!(rows.metadata.col_specs.len(), 1);
    assert_debug_snapshot!("select json", rows);
}

#[test]
fn select_simple_data() {
    let mut session = session();
    let _ = exec!(
        session,
        "insert into cycling.cyclist_name (id, lastname, firstname, records) values (1, 'john', 'johnson', {'f1': '120', 'f2': '126'});"
    );
    let _ = exec!(
        session,
        "insert into cycling.cyclist_name (id, lastname, firstname, records) values (2, 'smith', 'smithson', {'f1': '120', 'f2': '126'});"
    );

    let QueryResult::Rows(rows) = exec!(
        session,
        "select firstname as name, toJson(records) as rec from cycling.cyclist_name where id = 2;"
    ) else {
        panic!("invalid return type");
    };

    assert_debug_snapshot!("select single row", rows);
}
