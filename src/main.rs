use anyhow::Result;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

use datafusion::functions::datetime::date_trunc;
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Load all 2025 parquet files from /data
    ctx.register_parquet("yellow_trips", "data/*.parquet", ParquetReadOptions::default())
        .await?;

    println!("Parquet files registered successfully!\n");

    // Filter only 2025 (important to remove weird years)
    let base = ctx
        .table("yellow_trips")
        .await?
        .filter(
            col("tpep_pickup_datetime")
                .gt_eq(lit("2025-01-01"))
                .and(col("tpep_pickup_datetime").lt(lit("2026-01-01"))),
        )?;

    // =========================================================
    // Aggregation 1 (DataFrame API): Trips & revenue by month
    // =========================================================
    let pickup_month = date_trunc().call(vec![
        Expr::Literal(ScalarValue::Utf8(Some("month".to_string()))),
        col("tpep_pickup_datetime"),
    ]);

    let agg1_df = base
        .clone()
        .with_column("pickup_month", pickup_month)?
        .aggregate(
            vec![col("pickup_month")],
            vec![
                count(col("tpep_pickup_datetime")).alias("trip_count"),
                sum(col("total_amount")).alias("total_revenue"),
                avg(col("fare_amount")).alias("avg_fare"),
            ],
        )?
        .sort(vec![col("pickup_month").sort(true, true)])?;

    println!("Aggregation 1: Trips and Revenue by Month (DataFrame API) - 2025\n");
    print_batches(&agg1_df.collect().await?)?;
    println!();

    // =========================================================
    // Aggregation 1 (SQL)
    // =========================================================
    let sql1 = r#"
        SELECT
            date_trunc('month', tpep_pickup_datetime) AS pickup_month,
            COUNT(tpep_pickup_datetime) AS trip_count,
            SUM(total_amount) AS total_revenue,
            AVG(fare_amount) AS avg_fare
        FROM yellow_trips
        WHERE tpep_pickup_datetime >= '2025-01-01'
          AND tpep_pickup_datetime <  '2026-01-01'
        GROUP BY 1
        ORDER BY pickup_month ASC
    "#;

    println!("Aggregation 1: Trips and Revenue by Month (SQL) - 2025\n");
    print_batches(&ctx.sql(sql1).await?.collect().await?)?;
    println!();

    // =========================================================
    // Aggregation 2 (DataFrame API): Tip behavior by payment type
    // NOTE: DataFusion v45 has an internal limitation/bug when doing
    // SUM(tip_amount)/SUM(total_amount) as an expression in DataFrame API.
    // So we print the DF API columns we can safely compute here.
    // =========================================================
    let agg2_df = base
        .clone()
        .aggregate(
            vec![col("payment_type")],
            vec![
                count(col("tpep_pickup_datetime")).alias("trip_count"),
                avg(col("tip_amount")).alias("avg_tip_amount"),
                sum(col("tip_amount")).alias("sum_tip_amount"),
                sum(col("total_amount")).alias("sum_total_amount"),
            ],
        )?
        .sort(vec![col("trip_count").sort(false, true)])?;

    println!("Aggregation 2: Tip behavior by payment type (DataFrame API) - 2025\n");
    print_batches(&agg2_df.collect().await?)?;
    println!();

    // =========================================================
    // Aggregation 2 (SQL): includes tip_rate = SUM(tip)/SUM(total)
    // =========================================================
    let sql2 = r#"
        SELECT
            payment_type,
            COUNT(tpep_pickup_datetime) AS trip_count,
            AVG(tip_amount) AS avg_tip_amount,
            (SUM(tip_amount) / SUM(total_amount)) AS tip_rate
        FROM yellow_trips
        WHERE tpep_pickup_datetime >= '2025-01-01'
          AND tpep_pickup_datetime <  '2026-01-01'
        GROUP BY payment_type
        ORDER BY trip_count DESC
    "#;

    println!("Aggregation 2: Tip behavior by payment type (SQL) - 2025\n");
    print_batches(&ctx.sql(sql2).await?.collect().await?)?;
    println!();

    println!("All aggregations completed successfully.");

    Ok(())
}
