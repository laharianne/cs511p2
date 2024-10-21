// TODO: You need to implement the query b.sql in this file.
use crate::utils::*;

extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;
use std::collections::HashMap;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Define table columns for supplier, nation, region, and orders
    let table_columns = HashMap::from([
        ("supplier".into(), vec!["s_suppkey", "s_nationkey", "s_name"]),
        ("nation".into(), vec!["n_nationkey", "n_regionkey"]),
        ("region".into(), vec!["r_regionkey", "r_name"]),
        ("orders".into(), vec!["o_custkey", "o_totalprice"]),
    ]);

    // CSVReaderNodes for the four tables
    let supplier_csvreader_node = build_csv_reader_node("supplier".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_csv_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node = build_csv_reader_node("region".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);

    // WHERE Node for region filtering by 'EUROPE'
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let region_name = df.column("r_name").unwrap();
            let mask = region_name.equal("EUROPE").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN between supplier and nation
    let hash_join_supplier_nation = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    // HASH JOIN between nation and region
    let hash_join_nation_region = HashJoinBuilder::new()
        .left_on(vec!["n_regionkey".into()])
        .right_on(vec!["r_regionkey".into()])
        .build();

    // HASH JOIN between supplier and orders
    let hash_join_supplier_orders = HashJoinBuilder::new()
        .left_on(vec!["s_suppkey".into()])
        .right_on(vec!["o_custkey".into()])
        .build();

    // GROUP BY Aggregate Node for total_order_value
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["s_name".to_string()])
        .set_aggregates(vec![
            ("o_totalprice".into(), vec!["sum".into()]),
        ]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let columns = vec![
                Series::new("s_name", df.column("s_name").unwrap()),
                Series::new("total_order_value", df.column("o_totalprice_sum").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["total_order_value"], true)
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    hash_join_supplier_nation.subscribe_to_node(&supplier_csvreader_node, 0);
    hash_join_supplier_nation.subscribe_to_node(&nation_csvreader_node, 1);

    hash_join_nation_region.subscribe_to_node(&hash_join_supplier_nation, 0);
    hash_join_nation_region.subscribe_to_node(&region_csvreader_node, 1);

    where_node.subscribe_to_node(&hash_join_nation_region, 0);
    hash_join_supplier_orders.subscribe_to_node(&where_node, 0);
    hash_join_supplier_orders.subscribe_to_node(&orders_csvreader_node, 1);

    groupby_node.subscribe_to_node(&hash_join_supplier_orders, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(supplier_csvreader_node);
    service.add(nation_csvreader_node);
    service.add(region_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(where_node);
    service.add(hash_join_supplier_nation);
    service.add(hash_join_nation_region);
    service.add(hash_join_supplier_orders);
    service.add(groupby_node);
    service.add(select_node);

    service
}
