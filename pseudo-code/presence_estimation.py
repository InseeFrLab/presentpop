""" 
Computes weighted aggregate at cell-level per hour of the day.
To be run date by date

Output three types of aggregates:
(a) Unweighted mobile counts (on scope)
Two weighted-aggregates to proximate resident population (Insee computation)
(b) fixed weights, computed beforehand, independently of the present imsis this day 
(c) fixed weights adjusted to sum to a constant population, dependent on the present imsis this day 
(NB: If there are no imsi present this day for a given home cell with X residents -> loosing X residents)
"""

import sys
import datetime
import pyspark.sql.functions as psql_f
import pyspark.sql.types as psql_t
import pyspark.sql.window as psql_wdw


dir_scope = "PATH_TO_IMSI_REGISTER_WITH_HOMECELL"
dir_weight = "PATH_TO_HOMECELL_REGISTER_WITH_WEIGHTS"

# Output of device_presence_panel.py is the input here #
dir_input = "PATH_TO_INTERMEDIATE_TABLES_FOR_DEVICE_PRESENCE_PANEL"
# Scope: (Orange, Phones, at least present once in the day period)
# Day Period: DAY 1, 0am to DAY 1 + 1 DAY, 5am.

# Output present population estimates
dir_out = "PATH_TO_OUTPUT_ESTIMATES"


def hourly_pop_cell(spark, psql_f, psql_wdw, day, imsi_weights, dir_input, dir_out):

    imsi_refhour = spark.read.parquet(dir_input + day)

    # Restricting weights to the present pop this day
    imsi_weights = imsi_weights.join(imsi_refhour.select("imsi").distinct(), on="imsi")

    # ============= Adjust weights to map imsi to overall residents, and join. ======================#
    window_homecell = psql_wdw.Window().partitionBy("lac_home", "ci_sac_home")
    imsi_weights = imsi_weights.withColumn(
        "w_norm",
        psql_f.col("w")
        * psql_f.col("n_res_home_cell")
        / psql_f.sum(psql_f.col("w")).over(window_homecell),
    )

    imsi_refhour = imsi_refhour.join(imsi_weights, on="imsi")

    # ==================== Output aggregates ===============================#

    imsi_refhour = imsi_refhour.groupBy("ref_hour", "lac", "ci_sac").agg(
        psql_f.count("*").alias("imsi_count"),
        psql_f.sum("w").alias("res_count"),
        psql_f.sum("w_norm").alias("res_count_fixed_w"),
        psql_f.mean("diff_hour").alias("mean_diff_hour"),
        psql_f.sum(psql_f.col("w_norm") * psql_f.col("diff_hour")).alias(
            "sum_diff_hour_res"
        ),
    )

    print("Exporting results")

    imsi_refhour.repartition(1).write.parquet(dir_out + day)

    print("Exporting done")


if __name__ == "__main__":

    from pyspark.sql import SparkSession

    name_app = "Presence estimation"

    spark = SparkSession.builder.appName(name_app).getOrCreate()

    imsi_scope = (
        spark.read.parquet(dir_scope)
        .where(psql_f.col("in_scope") == True)
        .drop("in_scope")
    )

    ## == Weights == ##
    schema_w = psql_t.StructType(
        [
            psql_t.StructField("lac", psql_t.IntegerType(), True),
            psql_t.StructField("ci_sac", psql_t.IntegerType(), True),
            psql_t.StructField("w", psql_t.DoubleType(), True),
        ]
    )
    weights = (
        spark.read.schema(schema_w)
        .format("csv")
        .option("header", True)
        .load(dir_weight)
    )

    imsi_weights = imsi_scope.join(weights, on=["lac", "ci_sac"]).select(
        "imsi",
        "w",
        psql_f.col("lac").alias("lac_home"),
        psql_f.col("ci_sac").alias("ci_sac_home"),
    )

    ## == Residents per home cell == ##
    window_homecell = psql_wdw.Window().partitionBy("lac_home", "ci_sac_home")
    imsi_weights = imsi_weights.withColumn(
        "n_res_home_cell", psql_f.sum("w").over(window_homecell)
    )

    ## == Presence estimation == ##
    hourly_pop_cell(
        spark=spark,
        psql_f=psql_f,
        psql_wdw=psql_wdw,
        day=day,
        imsi_weights=imsi_weights,
        dir_input=dir_input,
        dir_out=dir_out,
    )
