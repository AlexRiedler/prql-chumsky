use itertools::Itertools;
use sqlformat::{format, FormatOptions, QueryParams};
use sqlparser::ast::{
    self as sql_ast, BinaryOperator, DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr,
    Join, JoinConstraint, JoinOperator, ObjectName, OrderByExpr, Select, SelectItem, SetExpr,
    TableAlias, TableFactor, TableWithJoins, Top, UnaryOperator, Value, WindowFrameBound,
    WindowSpec,
};

use super::{Transform, Query};
use anyhow::{anyhow, bail, Result};

#[derive(Debug)]
struct Context {
}

pub fn translate(query: Query) -> Result<String> {
    let sql_query = translate_query(query)?;

    let sql_query_string = sql_query.to_string();

    let formatted = format(
        &sql_query_string,
        &QueryParams::default(),
        FormatOptions::default(),
    );

    Ok(formatted)
}

pub fn translate_query(query: Query) -> Result<sql_ast::Query> {
    // extract tables and the pipeline
    let tables = query.tables;

    let (table, _) = &tables[0];

    let transforms = &table.transforms;

    let aggregates_position = transforms
        .iter()
        .position(|(t,_)| matches!(t, Transform::Aggregate(_)))
        .unwrap_or(transforms.len());
    let (before, after) = transforms.split_at(aggregates_position);

    Ok(
        sql_ast::Query {
            body: SetExpr::Select(
                      Box::new(Select {
                          top: None,
                          distinct: false,
                          projection: vec![],
                          into: None,
                          from: vec![], 
                          lateral_views: vec![],
                          selection: None,
                          group_by: vec![],
                          cluster_by: vec![],
                          distribute_by: vec![],
                          sort_by: vec![],
                          having: None,
                          qualify: None,
                      })
                  ),
                  order_by: vec![],
                  with: None,
                  limit: None,
                  offset: None,
                  fetch: None,
                  lock: None,

        }
    )
}

fn filter_of_pipeline(pipeline: &[Transform]) -> Result<Option<Expr>> {
    let filters: Vec<super::Expr> = pipline
        .iter()
        .filter_map(|t| match t {
            Transform::Filter(filter) => Some(*filter.clone()),
            _ => None,
        })
        .collect();

    filter_of_filters(filters);
}

fn filter_of_filters(conditions: Vec<super::Expr>) -> Result<Option<Expr>> {
    let mut condition = None;
    for filter in conditions {
        match filter {
            super::Expr::Bool(a, op, b) => Some(Node::from(
                    Item::Binary {
                        op: BigOp::And,
        }
    }
 

    condition.map(|n| n.item.try_into()).transpose()
}
}
