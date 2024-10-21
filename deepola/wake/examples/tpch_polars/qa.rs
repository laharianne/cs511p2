use crate::utils::*;

extern crate wake;
use polars::datatypes::BooleanChunked;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;

use std::collections::HashMap;

// This node implements the following SQL query
// SELECT
//     sum(p_retailprice) AS total
// FROM
//     part
// WHERE
//     p_retailprice > 1000
//     AND p_size IN (5, 10, 15, 20)
//     AND p_container <> 'JUMBO JAR'
//     AND p_mfgr LIKE 'Manufacturer#1%';

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        ("part".into(), vec!["p_retailprice", "p_size", "p_container", "p_mfgr"]),
    ]);

    // CSVReaderNode would be created for this table.
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);
    
    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            println!("WHERE node filtering: {:?}", df.shape());

            let price_mask = df.column("p_retailprice").unwrap().gt(1000.0).unwrap();
            let size_mask = df.column("p_size").unwrap()
                .equal(5).unwrap()
                | df.column("p_size").unwrap().equal(10).unwrap()
                | df.column("p_size").unwrap().equal(15).unwrap()
                | df.column("p_size").unwrap().equal(20).unwrap();
            let container_mask = df.column("p_container").unwrap()
                .utf8().unwrap().not_equal("JUMBO JAR");
            let mfgr_mask = df.column("p_mfgr").unwrap()
                .utf8().unwrap().into_iter()
                .map(|opt_s| opt_s.map_or(false, |s| s.starts_with("Manufacturer#1")))
                .collect::<BooleanChunked>();

            let mask = price_mask & size_mask & container_mask & mfgr_mask;
            df.filter(&mask).unwrap()
        })))
        .build();

    // Dummy Group Key (For AGGREGATE Node)
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let dummy_column = Series::new("dummy_group_key", vec![1; df.height()]);
            df.hstack(&[dummy_column]).unwrap()
        })))
        .build();
    
    // AGGREGATE Node
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["dummy_group_key".to_string()])
        .set_aggregates(vec![
            ("p_retailprice".into(), vec!["sum".into()]),
    ]);

    let aggregate_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();
    
    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let res = Series::new("total", df.column("p_retailprice_sum").unwrap());
            DataFrame::new(vec![res]).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&part_csvreader_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    aggregate_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&aggregate_node, 0);

    // Output reader subscribe to output node
    output_reader.subscribe_to_node(&select_node, 0);
    
    // Add all nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(select_node);
    service.add(aggregate_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(part_csvreader_node);
    service
}
