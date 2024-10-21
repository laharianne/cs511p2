use crate::utils::*;
extern crate wake;
use polars::prelude::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::ChunkCompare;
use polars::series::Series;
use wake::graph::*;
use wake::polars_operations::*;
use std::collections::HashMap;

/// This node implements the following SQL query
/// select
///   sum(l_extendedprice * (1 - l_discount)) as revenue
/// from
///   lineitem,
///   part
/// where
///   p_partkey = l_partkey
///   and l_shipinstruct = 'DELIVER IN PERSON'
///   and (
///     (p_brand = 'Brand#12' and l_quantity between 1 and 11 and p_size between 1 and 5)
///     or (p_brand = 'Brand#23' and l_quantity between 10 and 20 and p_size between 1 and 10)
///     or (p_brand = 'Brand#34' and l_quantity between 20 and 30 and p_size between 1 and 15)
///   );

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Define which columns we need from each table
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec!["l_partkey", "l_extendedprice", "l_discount", "l_shipinstruct", "l_quantity"],
        ),
        (
            "part".into(),
            vec!["p_partkey", "p_brand", "p_size"]
        ),
    ]);

    // Create CSVReaderNode for the two tables
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_csv_reader_node("part".into(), &tableinput, &table_columns);

    // WHERE node to filter `l_shipinstruct = 'DELIVER IN PERSON'`
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let shipinstruct = df.column("l_shipinstruct").unwrap();
            let mask = shipinstruct.equal("DELIVER IN PERSON").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // HASH JOIN node to join `lineitem` and `part` on `l_partkey = p_partkey`
    let hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    // Expression node to apply the filtering logic for `p_brand`, `l_quantity`, and `p_size`
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_brand = df.column("p_brand").unwrap();
            let l_quantity = df.column("l_quantity").unwrap();
            let p_size = df.column("p_size").unwrap();

            // Create the masks for each brand condition
            let mask = (p_brand.equal("Brand#12").unwrap()
                & l_quantity.gt_eq(1).unwrap() & l_quantity.lt_eq(11).unwrap()
                & p_size.gt_eq(1).unwrap() & p_size.lt_eq(5).unwrap())
                | (p_brand.equal("Brand#23").unwrap()
                & l_quantity.gt_eq(10).unwrap() & l_quantity.lt_eq(20).unwrap()
                & p_size.gt_eq(1).unwrap() & p_size.lt_eq(10).unwrap())
                | (p_brand.equal("Brand#34").unwrap()
                & l_quantity.gt_eq(20).unwrap() & l_quantity.lt_eq(30).unwrap()
                & p_size.gt_eq(1).unwrap() & p_size.lt_eq(15).unwrap());

            // Combine the masks using OR condition
            //let mask = brand12_mask | brand23_mask | brand34_mask;
            df.filter(&mask).unwrap()
        })))
        .build();

    // Add a node to calculate `l_extendedprice * (1 - l_discount)` for the final sum
    let revenue_calculation_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // let extended_price = df.column("l_extendedprice").unwrap().cast(&polars::datatypes::DataType::Float64).unwrap();
            // let discount = df.column("l_discount").unwrap().cast(&polars::datatypes::DataType::Float64).unwrap();
            // let revenue_series = extended_price * (discount * -1f64 + 1f64);
            // df.hstack(&[Series::new("revenue", revenue_series)]).unwrap()

            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let columns = vec![
                Series::new(
                    "disc_price",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64),
                )
            ];
            df.hstack(&columns).unwrap()

        })))
        .build();

    // AGGREGATE node to sum the revenue
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["l_partkey".to_string()])
        .set_aggregates(vec![
            ("disc_price".into(), vec!["sum".into()])
        ]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT node to return the result as a DataFrame
    // let select_node = AppenderNode::<DataFrame, MapAppender>::new()
    //     .appender(MapAppender::new(Box::new(|df: &DataFrame| {
    //         let total_revenue = df.column("revenue").unwrap().sum::<f64>().unwrap();
    //         DataFrame::new(vec![Series::new("revenue", vec![total_revenue])]).unwrap()
    //     })))
    //     .build();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
    .appender(MapAppender::new(Box::new(|df: &DataFrame| {
        // log::info!("Select Node!");
        let columns = vec![
            Series::new("revenue", df.column("disc_price_sum").unwrap()),
        ];
        DataFrame::new(columns)
            .unwrap()
            .sort(&["revenue"], true)
            .unwrap()
            .sum()
    })))
    .build();

    // Connect the nodes
    where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    hash_join_node.subscribe_to_node(&where_node, 0); // Left Node (lineitem)
    hash_join_node.subscribe_to_node(&part_csvreader_node, 1); // Right Node (part)
    expression_node.subscribe_to_node(&hash_join_node, 0);
    revenue_calculation_node.subscribe_to_node(&expression_node, 0);
    groupby_node.subscribe_to_node(&revenue_calculation_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node
    output_reader.subscribe_to_node(&select_node, 0);

    // Build the execution service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(lineitem_csvreader_node);
    service.add(part_csvreader_node);
    service.add(where_node);
    service.add(hash_join_node);
    service.add(expression_node);
    service.add(revenue_calculation_node);
    service.add(groupby_node);
    service.add(select_node);

    service
}