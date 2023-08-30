use std::fmt;

use eyre::Result;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{multispace0, multispace1},
    combinator::{map, opt},
    multi::{many_till, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded, separated_pair, terminated, tuple},
    IResult,
};
use serde::Serialize;

use crate::{
    cql::{
        literal::Literal,
        types::{parse_cql_type, PreCqlType},
    },
    frame::response::error::Error,
    parse,
    parse::{cassandra_type, ws},
};

#[derive(Debug, Clone, Serialize)]
pub enum QueryString {
    Select {
        keyspace: Option<String>,
        table: String,
        columns: SelectExpression,
        closure: Option<WhereClosure>,
    },
    Insert {
        keyspace: Option<String>,
        table: String,
        columns: Vec<String>,
        values: Vec<QueryValue>,
    },
    Delete {
        keyspace: Option<String>,
        table: String,
        columns: Vec<String>,
        values: Vec<QueryValue>,
    },
    Use {
        keyspace: String,
    },
    CreateKeyspace {
        keyspace: String,
        ignore_existence: bool,
        replication: Literal,
    },
    CreateTable {
        keyspace: Option<String>,
        table: String,
        ignore_existence: bool,
        columns: Vec<(String, PreCqlType)>,
        partition_keys: Vec<String>,
        clustering_keys: Vec<String>,
        options: Vec<(String, Literal)>,
    },
    CreateType {
        keyspace: Option<String>,
        table: String,
        columns: Vec<(String, String)>,
    },
}

impl QueryString {
    pub fn parse(query: &str) -> Result<Self, Error> {
        Ok(alt((
            use_query,
            select_query,
            insert_query,
            update_query,
            delete_query,
            create_keyspace_query,
            create_table_query,
            create_udt_query,
        ))(query)
        .map(|(_, it)| it)?)
    }

    pub fn keyspace(&self) -> Option<&str> {
        match self {
            QueryString::Select { keyspace, .. } => keyspace.as_deref(),
            QueryString::Insert { keyspace, .. } => keyspace.as_deref(),
            QueryString::Delete { keyspace, .. } => keyspace.as_deref(),
            QueryString::Use { keyspace, .. } => Some(keyspace),
            QueryString::CreateKeyspace { keyspace, .. } => Some(keyspace),
            QueryString::CreateTable { keyspace, .. } => keyspace.as_deref(),
            QueryString::CreateType { keyspace, .. } => keyspace.as_deref(),
        }
    }

    pub fn table(&self) -> Option<&str> {
        match self {
            QueryString::Select { table, .. } => Some(table),
            QueryString::Insert { table, .. } => Some(table),
            QueryString::Delete { table, .. } => Some(table),
            QueryString::CreateTable { table, .. } => Some(table),
            QueryString::CreateType { table, .. } => Some(table),
            _ => None,
        }
    }

    pub fn encode(&self) -> String {
        match self {
            QueryString::Select { .. } => "SELECT ".to_string(),
            QueryString::Insert { .. } => String::new(),
            QueryString::Delete { .. } => String::new(),
            _ => unimplemented!(),
        }
    }
}

impl fmt::Display for QueryString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryString::Select {
                keyspace,
                table,
                columns,
                closure,
            } => {
                write!(f, "SELECT {} FROM ", columns)?;
                if let Some(keyspace) = keyspace {
                    write!(f, "{}.", keyspace)?;
                }
                write!(f, "{}", table)?;
                if let Some(closure) = closure {
                    write!(f, " WHERE {}", closure)?;
                }
                Ok(())
            }
            QueryString::Insert {
                keyspace,
                table,
                columns,
                values,
            } => {
                write!(f, "INSERT INTO ")?;
                if let Some(keyspace) = keyspace {
                    write!(f, "{}.", keyspace)?;
                }
                write!(f, "{} ({}) VALUES (", table, columns.join(", "),)?;

                for value in values {
                    write!(f, "{}, ", value)?;
                }
                write!(f, ")")?;

                Ok(())
            }
            QueryString::Delete {
                keyspace,
                table,
                columns,
                values,
            } => {
                write!(f, "DELETE FROM")?;
                if let Some(keyspace) = keyspace {
                    write!(f, "{}.", keyspace)?;
                }
                write!(f, "{} WHERE ", table,)?;
                for (k, v) in columns.iter().zip(values.iter()) {
                    write!(f, "{} = {}, ", k, v)?;
                }
                Ok(())
            }
            QueryString::Use { keyspace } => write!(f, "USE {keyspace}"),
            _ => write!(f, "unimplemented debug string"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum SelectExpression {
    All,
    Columns(Vec<String>),
}

impl SelectExpression {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        let all = map(tag("*"), |_| Self::All);

        let columns = map(
            separated_list0(pair(tag(","), multispace0), parse::identifier),
            Self::Columns,
        );

        alt((all, columns))(input)
    }
}

impl fmt::Display for SelectExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectExpression::All => write!(f, "*"),
            SelectExpression::Columns(columns) => write!(f, "{}", columns.join(", ")),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WhereClosure {
    pub statements: Vec<(String, QueryValue)>,
}

impl WhereClosure {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        let (rest, _) = terminated(alt((tag("where"), tag("WHERE"))), multispace1)(input)?;

        let statement = separated_pair(parse::identifier, ws(tag("=")), QueryValue::parse);

        let (rest, statements) = separated_list1(ws(tag("AND")), statement)(rest)?;

        Ok((rest, Self { statements }))
    }
}

impl fmt::Display for WhereClosure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, value) in &self.statements {
            write!(f, "{} = {}", name, value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum QueryValue {
    Literal(Literal),
    Blankslate,
}

impl QueryValue {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        let blank = map(tag("?"), |_| Self::Blankslate);
        let named_bind = map(preceded(tag(":"), parse::identifier), |_| Self::Blankslate);
        let literal = map(Literal::parse, Self::Literal);
        alt((blank, literal, named_bind))(input)
    }
}

impl fmt::Display for QueryValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryValue::Literal(l) => write!(f, "{l}"),
            QueryValue::Blankslate => write!(f, "?"),
        }
    }
}

fn select_query(input: &str) -> IResult<&str, QueryString> {
    let (rest, _) = pair(alt((tag("select"), tag("SELECT"))), multispace1)(input)?;

    let (rest, columns) = SelectExpression::parse(rest)?;
    let (rest, _) = delimited(multispace0, alt((tag("from"), tag("FROM"))), multispace0)(rest)?;
    let (rest, keyspace) = opt(terminated(parse::identifier, tag(".")))(rest)?;
    let (rest, table) = terminated(parse::identifier, multispace0)(rest)?;

    let (rest, closure) = opt(WhereClosure::parse)(rest)?;

    Ok((
        rest,
        QueryString::Select {
            table,
            keyspace,
            columns,
            closure,
        },
    ))
}

fn insert_query(input: &str) -> IResult<&str, QueryString> {
    let (rest, _) = terminated(tag_no_case("insert into"), multispace1)(input)?;
    let (rest, keyspace) = opt(terminated(parse::identifier, tag(".")))(rest)?;
    let (rest, table) = terminated(parse::identifier, multispace0)(rest)?;
    let (rest, columns) = terminated(
        delimited(
            ws(tag("(")),
            separated_list0(ws(tag(",")), parse::identifier),
            ws(tag(")")),
        ),
        multispace0,
    )(rest)?;
    let (rest, _) = terminated(alt((tag("values"), tag("VALUES"))), multispace0)(rest)?;
    let (rest, values) = terminated(
        delimited(
            ws(tag("(")),
            separated_list0(ws(tag(",")), QueryValue::parse),
            ws(tag(")")),
        ),
        multispace0,
    )(rest)?;

    Ok((
        rest,
        QueryString::Insert {
            table,
            keyspace,
            columns,
            values,
        },
    ))
}

fn use_query(input: &str) -> IResult<&str, QueryString> {
    let (rest, _) = terminated(alt((tag("use"), tag("USE"))), multispace1)(input)?;
    let (rest, keyspace) = alt((
        parse::identifier,
        delimited(tag("\""), parse::identifier, tag("\"")),
    ))(rest)?;
    Ok((rest, QueryString::Use { keyspace }))
}

fn create_keyspace_query(input: &str) -> IResult<&str, QueryString> {
    let (rest, _) = terminated(
        alt((tag("create keyspace"), tag("CREATE KEYSPACE"))),
        multispace1,
    )(input)?;
    let (rest, if_not_exists) = opt(terminated(tag_no_case("IF NOT EXISTS"), multispace1))(rest)?;

    let (rest, keyspace) = terminated(parse::identifier, multispace1)(rest)?;
    let (rest, _) = terminated(alt((tag("with"), tag("WITH"))), multispace1)(rest)?;
    let replication = alt((tag("replication"), tag("REPLICATION")));

    let (rest, (_, replication)) = separated_pair(replication, ws(tag("=")), Literal::parse)(rest)?;

    Ok((
        rest,
        QueryString::CreateKeyspace {
            keyspace,
            ignore_existence: if_not_exists.is_some(),
            replication,
        },
    ))
}

fn create_table_query(rest: &str) -> IResult<&str, QueryString> {
    fn table_options(rest: &str) -> IResult<&str, Vec<(String, Literal)>> {
        let ordering = map(
            preceded(
                tag("CLUSTERING ORDER BY"),
                delimited(
                    ws(tag("(")),
                    separated_list1(
                        ws(tag(",")),
                        pair(
                            parse::identifier,
                            alt((
                                map(ws(tag("ASC")), |_| Literal::Bool(true)),
                                map(ws(tag("DESC")), |_| Literal::Bool(false)),
                            )),
                        ),
                    ),
                    ws(tag(")")),
                ),
            ),
            |t| {
                (
                    "clustering order by".to_owned(),
                    Literal::Map(t.into_iter().collect()),
                )
            },
        );

        let compact_storage = map(tag("COMPACT STORAGE"), |_| {
            ("compact storage".to_owned(), Literal::Bool(true))
        });

        let key_value = separated_pair(parse::identifier, ws(tag("=")), Literal::parse);

        separated_list1(ws(tag("AND")), alt((ordering, compact_storage, key_value)))(rest)
    }

    fn column_definition(rest: &str) -> IResult<&str, (String, PreCqlType, Option<&str>)> {
        tuple((
            terminated(parse::identifier, multispace0),
            terminated(parse_cql_type, multispace0),
            opt(terminated(tag("PRIMARY KEY"), multispace0)),
        ))(rest)
    }

    fn column_definition_without_primary(rest: &str) -> IResult<&str, (String, PreCqlType)> {
        tuple((
            terminated(parse::identifier, multispace0),
            terminated(parse_cql_type, multispace0),
        ))(rest)
    }

    // Vec<PartitionKeys> + Vec<ClusteringKeys>
    fn primary_key_definition(rest: &str) -> IResult<&str, (Vec<String>, Vec<String>)> {
        let partition_key = delimited(
            ws(tag("(")),
            separated_list1(ws(tag(",")), parse::identifier),
            ws(tag(")")),
        );
        let composite_key = delimited(
            ws(tag("(")),
            pair(
                terminated(partition_key, opt(ws(tag(",")))),
                separated_list0(ws(tag(",")), parse::identifier),
            ),
            ws(tag(")")),
        );

        // PRIMARY KEY (ident, ident)
        let compound_key = map(
            delimited(
                ws(tag("(")),
                separated_list1(ws(tag(",")), parse::identifier),
                ws(tag(")")),
            ),
            |it| {
                let (head, tail) = it.split_first().unwrap();

                (vec![head.clone()], tail.to_vec())
            },
        );

        let mut primary_key_definition =
            preceded(ws(tag("PRIMARY KEY")), alt((composite_key, compound_key)));

        primary_key_definition(rest)
    }

    let (rest, _) = terminated(alt((tag("create table"), tag("CREATE TABLE"))), multispace1)(rest)?;
    let (rest, if_not_exists) = opt(terminated(tag_no_case("IF NOT EXISTS"), multispace1))(rest)?;
    let (rest, keyspace) = opt(terminated(parse::identifier, tag(".")))(rest)?;
    let (rest, table) = terminated(parse::identifier, multispace0)(rest)?;

    let with_primary_key_definition = map(
        many_till(
            terminated(column_definition_without_primary, ws(tag(","))),
            primary_key_definition,
        ),
        |(c, (pk, ck))| (c, pk, ck),
    );
    let with_primary_key_inline = map(separated_list1(ws(tag(",")), column_definition), |c| {
        let pk = c
            .iter()
            .find_map(|(name, _, pk)| {
                if pk.is_some() {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .into_iter()
            .collect();
        let columns = c.into_iter().map(|(n, t, _)| (n, t)).collect();
        (columns, pk, vec![])
    });

    let (rest, (columns, primary_key, clustering_keys)) = delimited(
        ws(tag("(")),
        alt((with_primary_key_definition, with_primary_key_inline)),
        ws(tag(")")),
    )(rest)?;

    let (rest, options) = opt(preceded(ws(tag("WITH")), table_options))(rest)?;

    Ok((
        rest,
        QueryString::CreateTable {
            keyspace,
            table,
            ignore_existence: if_not_exists.is_some(),
            columns,
            partition_keys: primary_key,
            clustering_keys,
            options: options.unwrap_or_default(),
        },
    ))
}

fn update_query(rest: &str) -> IResult<&str, QueryString> {
    let (rest, _) = terminated(tag_no_case("update"), multispace1)(rest)?;
    let (rest, keyspace) = opt(terminated(parse::identifier, tag(".")))(rest)?;
    let (rest, table) = terminated(parse::identifier, multispace1)(rest)?;
    let (rest, _) = terminated(tag_no_case("set"), multispace1)(rest)?;

    let (rest, columns_specification) = terminated(
        separated_list1(
            ws(tag(",")),
            separated_pair(parse::identifier, ws(tag("=")), QueryValue::parse),
        ),
        multispace1,
    )(rest)?;
    let (rest, _) = terminated(tag_no_case("where"), multispace1)(rest)?;

    let (rest, row_specification) = terminated(
        separated_list1(
            ws(tag("AND")),
            separated_pair(parse::identifier, ws(tag("=")), QueryValue::parse),
        ),
        multispace0,
    )(rest)?;

    let (columns, values) = columns_specification
        .into_iter()
        .chain(row_specification)
        .unzip();

    Ok((
        rest,
        QueryString::Insert {
            table,
            keyspace,
            columns,
            values,
        },
    ))
}

fn delete_query(rest: &str) -> IResult<&str, QueryString> {
    let (rest, _) = terminated(tag_no_case("delete from"), multispace1)(rest)?;
    let (rest, keyspace) = opt(terminated(parse::identifier, tag(".")))(rest)?;
    let (rest, table) = terminated(parse::identifier, multispace1)(rest)?;
    let (rest, _) = terminated(tag_no_case("where"), multispace1)(rest)?;

    let (rest, row_specification) = terminated(
        separated_list1(
            ws(tag("AND")),
            separated_pair(parse::identifier, ws(tag("=")), QueryValue::parse),
        ),
        multispace0,
    )(rest)?;

    let (columns, values) = row_specification.into_iter().unzip();

    Ok((
        rest,
        QueryString::Delete {
            table,
            keyspace,
            columns,
            values,
        },
    ))
}

fn create_udt_query(rest: &str) -> IResult<&str, QueryString> {
    let (rest, _) = terminated(tag_no_case("create type"), multispace1)(rest)?;
    let (rest, _) = opt(terminated(tag_no_case("if not exists"), multispace1))(rest)?;

    let (rest, keyspace) = opt(terminated(parse::identifier, tag(".")))(rest)?;
    let (rest, table) = terminated(parse::identifier, multispace0)(rest)?;

    let ident_type = tuple((
        terminated(parse::identifier, multispace0),
        terminated(cassandra_type, multispace0),
    ));
    let (rest, columns) = delimited(
        ws(tag("(")),
        separated_list1(ws(tag(",")), ident_type),
        ws(tag(")")),
    )(rest)?;

    Ok((
        rest,
        QueryString::CreateType {
            keyspace,
            table,
            columns,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::QueryString;

    #[test]
    fn test_select() {
        let q = "select keyspace_name, table_name, column_name, kind, position, type from system_schema.columns";
        let s = QueryString::parse(q).unwrap();
        println!("{s:#?}");
    }

    #[test]
    fn test_select_where() {
        let q = "SELECT field1,field2,field3 FROM table WHERE field0 = ?";
        let s = QueryString::parse(q).unwrap();
        println!("{s:#?}");
    }

    #[test]
    fn test_insert_into() {
        let q = "INSERT INTO table (field1,field2,field3,field4) VALUES (?,?,?,?)";
        let i = QueryString::parse(q).unwrap();
        println!("{i:#?}")
    }

    #[test]
    fn test_create_keyspace() {
        let q = "CREATE KEYSPACE keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
        let k = QueryString::parse(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_update_query() {
        let q = "UPDATE table SET field1=?,field2=?,field3=? WHERE field0=?";
        let k = QueryString::parse(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_delete_query() {
        let q = "DELETE FROM table WHERE field1=? AND field2=? AND field3=?";
        let k = QueryString::parse(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_create_table() {
        let q = r#"CREATE TABLE keyspace.table (
                field1 uuid,
                field2 text,
                field3 text,
                field4 text,
                field5 timestamp,
                field6 boolean,
                field7 text,
                field8 timestamp,
                PRIMARY KEY ((field1, field2))
            ) WITH bloom_filter_fp_chance = 0.01
                AND comment = ''
                AND parameter = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
                AND crc_check_chance = 1.0
                AND gc_grace_seconds = 1234
                AND speculative_retry = '99PERCENTILE'
        "#;

        let k = QueryString::parse(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_udt() {
        let q = r#"CREATE TYPE cycling.basic_info (
              birthday timestamp,
              nationality text,
              weight text,
              height text
            );
        "#;
        let k = QueryString::parse(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_named_bind() {
        let q = "INSERT INTO table (field1,field2,field3,field4,field5,field6) VALUES (:field1,:field2,:field3,:field4,:field5,:field6)";
        let k = QueryString::parse(q).unwrap();
        println!("{k:#?}")
    }

    #[test]
    fn test_update_with_lit_null() {
        let q = "UPDATE table SET field1=null,field2=null WHERE field3=? AND field4=?;";
        let k = QueryString::parse(q).unwrap();
        println!("{k:?}");
    }
}
