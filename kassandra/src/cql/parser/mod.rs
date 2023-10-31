use std::str::FromStr;

use eyre::Result;
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, multispace0},
    combinator::{map, opt, recognize},
    error::ParseError,
    multi::{many0_count, separated_list1},
    sequence::{delimited, pair},
    IResult,
};

use crate::{cql::query::QueryString, frame::response::error::Error};

pub fn query(query: &str) -> Result<QueryString, Error> {
    Ok(alt((
        queries::use_query,
        queries::select_query,
        queries::insert_query,
        queries::update_query,
        queries::delete_query,
        queries::create_keyspace_query,
        queries::create_table_query,
        queries::create_udt_query,
    ))(query)
    .map(|(_, it)| it)?)
}

impl FromStr for QueryString {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        query(s)
    }
}

pub fn identifier(input: &str) -> IResult<&str, String> {
    let ident = recognize(pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    ));

    map(ident, |it: &str| it.to_lowercase())(input)
}

pub fn cassandra_type(input: &str) -> IResult<&str, String> {
    let ident = pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    );
    let generics = opt(delimited(
        tag("<"),
        separated_list1(ws(tag(",")), ident),
        tag(">"),
    ));
    let ident = pair(
        alt((alpha1, tag("_"))),
        many0_count(alt((alphanumeric1, tag("_")))),
    );
    map(recognize(pair(ident, generics)), |it: &str| it.to_owned())(input)
}

pub fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

mod queries {
    use nom::{
        branch::alt,
        bytes::complete::{tag, tag_no_case},
        character::complete::{multispace0, multispace1, u32},
        combinator::{map, opt},
        multi::{many_till, separated_list0, separated_list1},
        sequence::{delimited, pair, preceded, separated_pair, terminated, tuple},
        IResult,
    };

    use super::{cassandra_type, identifier, ws};
    use crate::cql::{
        literal::Literal,
        query::{
            CreateKeyspaceQuery, CreateTableQuery, CreateTypeQuery, DeleteQuery, InsertQuery,
            QueryString, QueryValue, SelectExpression, SelectQuery, WhereClosure,
        },
        types::PreCqlType,
    };

    fn query_value(input: &str) -> IResult<&str, QueryValue> {
        let blank = map(tag("?"), |_| QueryValue::Blankslate);
        let named_bind = map(preceded(tag(":"), identifier), |_| QueryValue::Blankslate);
        let literal = map(super::literal::parse, QueryValue::Literal);
        alt((blank, literal, named_bind))(input)
    }

    fn select_expression(input: &str) -> IResult<&str, SelectExpression> {
        let all = map(tag("*"), |_| SelectExpression::All);

        let columns = map(
            separated_list0(pair(tag(","), multispace0), identifier),
            SelectExpression::Columns,
        );

        alt((all, columns))(input)
    }

    fn where_closure(input: &str) -> IResult<&str, WhereClosure> {
        let (rest, _) = terminated(tag_no_case("where"), multispace1)(input)?;

        let statement = separated_pair(identifier, ws(tag("=")), query_value);

        let (rest, statements) = separated_list1(ws(tag("AND")), statement)(rest)?;

        Ok((rest, WhereClosure { statements }))
    }

    pub fn select_query(input: &str) -> IResult<&str, QueryString> {
        let (rest, _) = terminated(tag_no_case("select"), multispace1)(input)?;
        let (rest, json) = map(opt(terminated(tag_no_case("json"), multispace1)), |it| {
            it.is_some()
        })(rest)?;

        let (rest, columns) = select_expression(rest)?;
        let (rest, _) = delimited(multispace0, alt((tag("from"), tag("FROM"))), multispace0)(rest)?;
        let (rest, keyspace) = opt(terminated(identifier, tag(".")))(rest)?;
        let (rest, table) = terminated(identifier, multispace0)(rest)?;

        let (rest, closure) = opt(terminated(where_closure, multispace0))(rest)?;
        // todo: order by
        let limit = preceded(
            terminated(tag_no_case("limit"), multispace1),
            terminated(map(u32, |it| it as usize), multispace0),
        );
        let (rest, limit) = opt(limit)(rest)?;

        Ok((
            rest,
            QueryString::Select(SelectQuery {
                table,
                keyspace,
                columns,
                r#where: closure.unwrap_or_default(),
                limit,
                json,
            }),
        ))
    }

    pub fn insert_query(input: &str) -> IResult<&str, QueryString> {
        let (rest, _) = terminated(tag_no_case("insert"), multispace1)(input)?;
        let (rest, _) = terminated(tag_no_case("into"), multispace1)(rest)?;
        let (rest, keyspace) = opt(terminated(identifier, tag(".")))(rest)?;
        let (rest, table) = terminated(identifier, multispace0)(rest)?;
        let (rest, columns) = terminated(
            delimited(
                ws(tag("(")),
                separated_list0(ws(tag(",")), identifier),
                ws(tag(")")),
            ),
            multispace0,
        )(rest)?;
        let (rest, _) = terminated(tag_no_case("values"), multispace0)(rest)?;
        let (rest, values) = terminated(
            delimited(
                ws(tag("(")),
                separated_list0(ws(tag(",")), query_value),
                ws(tag(")")),
            ),
            multispace0,
        )(rest)?;

        Ok((
            rest,
            QueryString::Insert(InsertQuery {
                table,
                keyspace,
                columns,
                values,
            }),
        ))
    }

    pub fn use_query(input: &str) -> IResult<&str, QueryString> {
        let (rest, _) = terminated(alt((tag("use"), tag("USE"))), multispace1)(input)?;
        let (rest, keyspace) =
            alt((identifier, delimited(tag("\""), identifier, tag("\""))))(rest)?;
        Ok((rest, QueryString::Use { keyspace }))
    }

    pub fn create_keyspace_query(input: &str) -> IResult<&str, QueryString> {
        let (rest, _) = terminated(
            alt((tag("create keyspace"), tag("CREATE KEYSPACE"))),
            multispace1,
        )(input)?;
        let (rest, if_not_exists) =
            opt(terminated(tag_no_case("IF NOT EXISTS"), multispace1))(rest)?;

        let (rest, keyspace) = terminated(identifier, multispace1)(rest)?;
        let (rest, _) = terminated(alt((tag("with"), tag("WITH"))), multispace1)(rest)?;
        let replication = alt((tag("replication"), tag("REPLICATION")));

        let (rest, (_, replication)) =
            separated_pair(replication, ws(tag("=")), super::literal::parse)(rest)?;

        Ok((
            rest,
            QueryString::CreateKeyspace(CreateKeyspaceQuery {
                keyspace,
                ignore_existence: if_not_exists.is_some(),
                replication,
            }),
        ))
    }

    pub fn create_table_query(rest: &str) -> IResult<&str, QueryString> {
        fn table_options(rest: &str) -> IResult<&str, Vec<(String, Literal)>> {
            let ordering = map(
                preceded(
                    tag("CLUSTERING ORDER BY"),
                    delimited(
                        ws(tag("(")),
                        separated_list1(
                            ws(tag(",")),
                            pair(
                                identifier,
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

            let key_value = separated_pair(identifier, ws(tag("=")), super::literal::parse);

            separated_list1(ws(tag("AND")), alt((ordering, compact_storage, key_value)))(rest)
        }

        fn column_definition(rest: &str) -> IResult<&str, (String, PreCqlType, Option<&str>)> {
            tuple((
                terminated(identifier, multispace0),
                terminated(super::types::parse, multispace0),
                opt(terminated(tag("PRIMARY KEY"), multispace0)),
            ))(rest)
        }

        fn column_definition_without_primary(rest: &str) -> IResult<&str, (String, PreCqlType)> {
            tuple((
                terminated(identifier, multispace0),
                terminated(super::types::parse, multispace0),
            ))(rest)
        }

        // Vec<PartitionKeys> + Vec<ClusteringKeys>
        fn primary_key_definition(rest: &str) -> IResult<&str, (Vec<String>, Vec<String>)> {
            let partition_key = delimited(
                ws(tag("(")),
                separated_list1(ws(tag(",")), identifier),
                ws(tag(")")),
            );
            let composite_key = delimited(
                ws(tag("(")),
                pair(
                    terminated(partition_key, opt(ws(tag(",")))),
                    separated_list0(ws(tag(",")), identifier),
                ),
                ws(tag(")")),
            );

            // PRIMARY KEY (ident, ident)
            let compound_key = map(
                delimited(
                    ws(tag("(")),
                    separated_list1(ws(tag(",")), identifier),
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

        let (rest, _) =
            terminated(alt((tag("create table"), tag("CREATE TABLE"))), multispace1)(rest)?;
        let (rest, if_not_exists) =
            opt(terminated(tag_no_case("IF NOT EXISTS"), multispace1))(rest)?;
        let (rest, keyspace) = opt(terminated(identifier, tag(".")))(rest)?;
        let (rest, table) = terminated(identifier, multispace0)(rest)?;

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
            QueryString::CreateTable(CreateTableQuery {
                keyspace,
                table,
                ignore_existence: if_not_exists.is_some(),
                columns,
                partition_keys: primary_key,
                clustering_keys,
                options: options.unwrap_or_default(),
            }),
        ))
    }

    pub fn update_query(rest: &str) -> IResult<&str, QueryString> {
        let (rest, _) = terminated(tag_no_case("update"), multispace1)(rest)?;
        let (rest, keyspace) = opt(terminated(identifier, tag(".")))(rest)?;
        let (rest, table) = terminated(identifier, multispace1)(rest)?;
        let (rest, _) = terminated(tag_no_case("set"), multispace1)(rest)?;

        let (rest, columns_specification) = terminated(
            separated_list1(
                ws(tag(",")),
                separated_pair(identifier, ws(tag("=")), query_value),
            ),
            multispace1,
        )(rest)?;
        let (rest, _) = terminated(tag_no_case("where"), multispace1)(rest)?;

        let (rest, row_specification) = terminated(
            separated_list1(
                ws(tag("AND")),
                separated_pair(identifier, ws(tag("=")), query_value),
            ),
            multispace0,
        )(rest)?;

        let (columns, values) = columns_specification
            .into_iter()
            .chain(row_specification)
            .unzip();

        Ok((
            rest,
            QueryString::Insert(InsertQuery {
                table,
                keyspace,
                columns,
                values,
            }),
        ))
    }

    pub fn delete_query(rest: &str) -> IResult<&str, QueryString> {
        let (rest, _) = terminated(tag_no_case("delete"), multispace1)(rest)?;

        let columns_list = terminated(separated_list1(ws(tag(",")), identifier), multispace1);

        let from_tag = terminated(tag_no_case("from"), multispace1);
        let from_tag_empty = map(terminated(tag_no_case("from"), multispace1), |_| vec![]);
        let (rest, columns) = alt((terminated(columns_list, from_tag), from_tag_empty))(rest)?;

        let (rest, keyspace) = opt(terminated(identifier, tag(".")))(rest)?;
        let (rest, table) = terminated(identifier, multispace1)(rest)?;
        let (rest, _) = terminated(tag_no_case("where"), multispace1)(rest)?;

        let (rest, statements) = terminated(
            separated_list1(
                ws(tag("AND")),
                separated_pair(identifier, ws(tag("=")), query_value),
            ),
            multispace0,
        )(rest)?;

        let r#where = WhereClosure { statements };

        Ok((
            rest,
            QueryString::Delete(DeleteQuery {
                table,
                keyspace,
                columns,
                r#where,
            }),
        ))
    }

    pub fn create_udt_query(rest: &str) -> IResult<&str, QueryString> {
        let (rest, _) = terminated(tag_no_case("create type"), multispace1)(rest)?;
        let (rest, _) = opt(terminated(tag_no_case("if not exists"), multispace1))(rest)?;

        let (rest, keyspace) = opt(terminated(identifier, tag(".")))(rest)?;
        let (rest, table) = terminated(identifier, multispace0)(rest)?;

        let ident_type = tuple((
            terminated(identifier, multispace0),
            terminated(cassandra_type, multispace0),
        ));
        let (rest, columns) = delimited(
            ws(tag("(")),
            separated_list1(ws(tag(",")), ident_type),
            ws(tag(")")),
        )(rest)?;

        Ok((
            rest,
            QueryString::CreateType(CreateTypeQuery {
                keyspace,
                name: table,
                columns,
            }),
        ))
    }
}

mod types {
    use std::str::FromStr;

    use nom::{
        bytes::{complete::tag, streaming::take_while},
        character::is_alphanumeric,
        error::ErrorKind,
        multi::separated_list1,
        sequence::terminated,
        IResult,
    };

    use super::{identifier, ws};
    use crate::cql::types::{NativeType, PreCqlType};

    type ParseResult<'a, T> = IResult<&'a str, T, nom::error::Error<&'a str>>;

    pub fn parse(p: &str) -> ParseResult<PreCqlType> {
        if let Ok((_rest, _)) = tag::<_, _, nom::error::Error<_>>("frozen<")(p) {
            let (p, inner_type) = parse(p)?;
            let frozen_type = inner_type.freeze();
            Ok((p, frozen_type))
        } else if let Ok((p, _)) = tag::<_, _, nom::error::Error<_>>("map<")(p) {
            let (p, key) = terminated(parse, ws(tag(",")))(p)?;
            let (p, value) = parse(p)?;
            let (p, _) = tag(">")(p)?;

            let typ = PreCqlType::Map {
                frozen: false,
                key: Box::new(key),
                value: Box::new(value),
            };

            Ok((p, typ))
        } else if let Ok((p, _)) = tag::<_, _, nom::error::Error<_>>("list<")(p) {
            let (p, inner_type) = parse(p)?;
            let (p, _) = tag(">")(p)?;

            let typ = PreCqlType::List {
                frozen: false,
                item: Box::new(inner_type),
            };

            Ok((p, typ))
        } else if let Ok((p, _)) = tag::<_, _, nom::error::Error<_>>("set<")(p) {
            let (p, inner_type) = parse(p)?;
            let (p, _) = tag(">")(p)?;

            let typ = PreCqlType::Set {
                frozen: false,
                item: Box::new(inner_type),
            };

            Ok((p, typ))
        } else if let Ok((p, _)) = tag::<_, _, nom::error::Error<_>>("tuple<")(p) {
            let (p, types) = separated_list1(ws(tag(",")), parse)(p)?;
            let (p, _) = tag(">")(p)?;
            Ok((p, PreCqlType::Tuple(types)))
        } else if let Ok((p, typ)) = parse_native_type(p) {
            Ok((p, PreCqlType::Native(typ)))
        } else if let Ok((name, p)) = parse_user_defined_type(p) {
            let typ = PreCqlType::UserDefinedType {
                frozen: false,
                name: name.to_string(),
            };
            Ok((p, typ))
        } else {
            // Err(p.error(ParseErrorCause::Other("invalid cql type")))
            panic!("invalid cql type")
        }
    }

    fn parse_native_type(p: &str) -> ParseResult<NativeType> {
        let (p, tok) = identifier(p)?;
        let typ = NativeType::from_str(&tok)
            .map_err(|_| nom::Err::Error(nom::error::make_error(p, ErrorKind::Tag)))?;
        Ok((p, typ))
    }

    fn parse_user_defined_type(p: &str) -> ParseResult<&str> {
        // Java identifiers allow letters, underscores and dollar signs at any position
        // and digits in non-first position. Dots are accepted here because the names
        // are usually fully qualified.
        let (p, tok) =
            take_while(|c| is_alphanumeric(c as u8) || c == '.' || c == '_' || c == '$')(p)?;

        if tok.is_empty() {
            return Err(nom::Err::Error(nom::error::make_error(p, ErrorKind::Tag)));
        }
        Ok((p, tok))
    }
}

mod literal {
    use std::str::FromStr;

    use nom::{
        branch::alt,
        bytes::complete::{tag, tag_no_case, take_until, take_while_m_n},
        character::complete::multispace0,
        combinator::{map, recognize},
        multi::separated_list0,
        sequence::{delimited, separated_pair, terminated, tuple},
        IResult,
    };
    use uuid::Uuid;

    use super::ws;
    use crate::cql::literal::Literal;

    pub fn parse(input: &str) -> IResult<&str, Literal> {
        alt((
            uuid_literal,
            null_literal,
            map_literal,
            string_literal,
            number_literal,
            float_literal,
            list_literal,
        ))(input)
    }

    fn string_literal(input: &str) -> IResult<&str, Literal> {
        map(
            delimited(tag("'"), take_until("'"), tag("'")),
            |it: &str| Literal::String(it.to_owned()),
        )(input)
    }

    fn number_literal(input: &str) -> IResult<&str, Literal> {
        map(nom::character::complete::i64, Literal::Number)(input)
    }

    fn null_literal(input: &str) -> IResult<&str, Literal> {
        map(tag_no_case("null"), |_| Literal::Null)(input)
    }

    fn float_literal(input: &str) -> IResult<&str, Literal> {
        map(nom::number::complete::double, Literal::Float)(input)
    }

    fn list_literal(input: &str) -> IResult<&str, Literal> {
        let values = separated_list0(ws(tag(",")), ws(parse));
        map(delimited(ws(tag("[")), values, ws(tag("]"))), Literal::List)(input)
    }

    fn map_literal(input: &str) -> IResult<&str, Literal> {
        let quoted_string = delimited(tag("'"), take_until("'"), tag("'"));
        let value = separated_pair(ws(quoted_string), tag(":"), ws(parse));

        let values = separated_list0(terminated(tag(","), multispace0), value);

        map(
            delimited(tag("{"), values, tag("}")),
            |it: Vec<(&str, Literal)>| {
                Literal::Map(
                    it.into_iter()
                        .map(|(key, value)| (key.to_owned(), value))
                        .collect(),
                )
            },
        )(input)
    }

    fn uuid_literal(input: &str) -> IResult<&str, Literal> {
        let lower_hex = tuple((
            take_while_m_n(8, 8, is_lower_hex_digit),
            tag("-"),
            take_while_m_n(4, 4, is_lower_hex_digit),
            tag("-"),
            take_while_m_n(4, 4, is_lower_hex_digit),
            tag("-"),
            take_while_m_n(4, 4, is_lower_hex_digit),
            tag("-"),
            take_while_m_n(12, 12, is_lower_hex_digit),
        ));
        let upper_hex = tuple((
            take_while_m_n(8, 8, is_upper_hex_digit),
            tag("-"),
            take_while_m_n(4, 4, is_upper_hex_digit),
            tag("-"),
            take_while_m_n(4, 4, is_upper_hex_digit),
            tag("-"),
            take_while_m_n(4, 4, is_upper_hex_digit),
            tag("-"),
            take_while_m_n(12, 12, is_upper_hex_digit),
        ));
        let parser = alt((lower_hex, upper_hex));
        let (rest, lit) = recognize(parser)(input)?;
        let uuid = Uuid::from_str(lit).expect("to be valid uuid");
        Ok((rest, Literal::Uuid(uuid)))
    }

    #[inline]
    fn is_lower_hex_digit(i: char) -> bool {
        ('a'..='f').contains(&i) || i.is_ascii_digit()
    }

    #[inline]
    fn is_upper_hex_digit(i: char) -> bool {
        ('A'..='F').contains(&i) || i.is_ascii_digit()
    }

    #[cfg(test)]
    mod tests {
        use super::{map_literal, parse};

        #[test]
        fn test_map() {
            let v = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
            let (_, m) = map_literal(v).unwrap();
            println!("{m:?}");
        }

        #[test]
        fn test_uuid() {
            let v = "6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47";
            let (_, m) = parse(v).unwrap();
            println!("{m:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::query;
    use crate::cql::query::QueryString;

    #[test]
    fn test_select() {
        let q = "select keyspace_name, table_name, column_name, kind, position, type from system_schema.columns";
        let s = query(q).unwrap();
        println!("{s:#?}");
    }

    #[test]
    fn test_select_where() {
        let q = "SELECT field1,field2,field3 FROM table WHERE field0 = ?";
        let s = query(q).unwrap();
        println!("{s:#?}");
    }

    #[test]
    fn test_select_where_limit() {
        let q = "SELECT field1,field2,field3 FROM table WHERE field0 = ? limit 500";
        let QueryString::Select(s) = query(q).unwrap() else {
            panic!("was supposed to be parsed as select query")
        };
        assert_eq!(s.limit, Some(500));
        println!("{s:#?}");
    }

    #[test]
    fn test_insert_into() {
        let q = "INSERT INTO table (field1,field2,field3,field4) VALUES (?,?,?,?)";
        let i = query(q).unwrap();
        println!("{i:#?}")
    }

    #[test]
    fn test_create_keyspace() {
        let q = "CREATE KEYSPACE keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
        let k = query(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_update_query() {
        let q = "UPDATE table SET field1=?,field2=?,field3=? WHERE field0=?";
        let k = query(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_delete_row_query() {
        let q = "DELETE FROM table WHERE field1=? AND field2=? AND field3=?";
        let k = query(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_delete_columns_query() {
        let q = "DELETE field4, field5 FROM table WHERE field1=? AND field2=? AND field3=?";
        let k = query(q).unwrap();
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

        let k = query(q).unwrap();
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
        let k = query(q).unwrap();
        println!("{k:#?}");
    }

    #[test]
    fn test_named_bind() {
        let q = "INSERT INTO table (field1,field2,field3,field4,field5,field6) VALUES (:field1,:field2,:field3,:field4,:field5,:field6)";
        let k = query(q).unwrap();
        println!("{k:#?}")
    }

    #[test]
    fn test_update_with_lit_null() {
        let q = "UPDATE table SET field1=null,field2=null WHERE field3=? AND field4=?;";
        let k = query(q).unwrap();
        println!("{k:?}");
    }

    #[test]
    fn test_regressions() {
        let qs = [ "CREATE TABLE cycling.cyclist_name (     id UUID PRIMARY KEY,     lastname text,     firstname text );", "INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'KRUIKSWIJK','Steven');"];

        for q in qs {
            let _ = query(q).unwrap();
        }
    }

    #[test]
    fn test_select_json() {
        let q = "SELECT JSON field1,field2,field3 FROM table WHERE field0 = ? limit 500";
        let QueryString::Select(s) = query(q).unwrap() else {
            panic!("was supposed to be parsed as select query")
        };
        assert!(s.json);
    }
}
