from pyspark.sql import functions as F

silver_table = "workspace.bigquery_db_cohort_db.silver_cohort_analysis"
gold_table = "workspace.bigquery_db_cohort_db.gold_time_to_second_purchase"

df = (
    spark.read.table(silver_table)
    .filter(F.col("second_purchase_date").isNotNull())
    .withColumn("cohort_month", F.date_trunc("month", "first_purchase_date"))
)

gold_df = (
    df
    .groupBy("cohort_month")
    .agg(
        F.round(F.avg("days_between_first_and_second")).alias("avg_days_to_repurchase"),
        F.round(F.expr("percentile(days_between_first_and_second, 0.5)")).alias("median_days_to_repurchase")
    )
    .orderBy("cohort_month")
)

(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)
