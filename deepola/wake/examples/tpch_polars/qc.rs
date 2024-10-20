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
    // Define table columns for customer, orders, and lineitem
    let table_columns = HashMap::from([
        ("customer".into(), vec!["c_custkey", "c_name", "c_acctbal"]),
        ("orders".into(), vec!["o_orderkey", "o_custkey", "o_orderdate"]),
        ("lineitem".into(), vec!["l_orderkey", "l_extendedprice", "l_discount"]),
    ]);

    // CSVReaderNodes for the three tables
    let customer_csvreader_node = build_csv_reader_node("customer".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // log::info!("Filtering data in WHERE node");
            let order_date = df.column("o_orderdate").unwrap();
            let mask = order_date.gt_eq("1993-10-01").unwrap() & order_date.lt("1994-01-01").unwrap();
            // let result = df.filter(&mask).unwrap();
            // log::info!("Filtered mask created");
            // let before_filter_count = df.height();
            // log::info!("Before filtering: {} rows", before_filter_count);
            let result = df.filter(&mask).unwrap();
            // let after_filter_count = result.height();
            // log::info!("After filtering: {} rows", after_filter_count);
            result
        })))
        .build();
    
    // HASH JOIN Node between customer and orders
    let hash_join_orders_customer = HashJoinBuilder::new()
        .left_on(vec!["c_custkey".into()])
        .right_on(vec!["o_custkey".into()])
        .build();

    // HASH JOIN Node between orders and lineitem
    let hash_join_lineitem_orders = HashJoinBuilder::new()
        .left_on(vec!["o_orderkey".into()])
        .right_on(vec!["l_orderkey".into()])
        .build();

    // EXPRESSION Node for calculating revenue
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // log::info!("Computing disc_price!");
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

    // GROUP BY Aggregate Node
    let mut sum_accumulator = SumAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["c_custkey".to_string(), "c_name".to_string(), "c_acctbal".to_string()])
        .set_aggregates(vec![
            ("disc_price".into(), vec!["sum".into()])
        ]);

    let groupby_node = AccumulatorNode::<DataFrame, SumAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // log::info!("Select Node!");
            let columns = vec![
                Series::new("c_custkey", df.column("c_custkey").unwrap()),
                Series::new("c_name", df.column("c_name").unwrap()),
                Series::new("c_acctbal", df.column("c_acctbal").unwrap()),
                Series::new("revenue", df.column("disc_price_sum").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["revenue"], true)
                .unwrap()
        })))
        .build();


    // Connect nodes with subscription
    // where_node.subscribe_to_node(&orders_csvreader_node, 0); 
    hash_join_orders_customer.subscribe_to_node(&customer_csvreader_node, 0); 
    hash_join_orders_customer.subscribe_to_node(&orders_csvreader_node, 1); 
    // where_node.subscribe_to_node(&hash_join_orders_customer, 0);
    hash_join_lineitem_orders.subscribe_to_node(&hash_join_orders_customer, 0); 
    hash_join_lineitem_orders.subscribe_to_node(&lineitem_csvreader_node, 1); 
    where_node.subscribe_to_node(&hash_join_lineitem_orders,0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0); 
    select_node.subscribe_to_node(&groupby_node, 0); 



    // Output reader subscribe to output node
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    // log::info!("Starting query execution");
    service.add(orders_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(lineitem_csvreader_node);
    service.add(where_node);
    service.add(hash_join_lineitem_orders);
    service.add(hash_join_orders_customer);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(select_node);
    service
}