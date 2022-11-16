"""
Residency characterisation (home cell algorithm)
Step 3 : From two-weeks max-presence cells to home cell
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as psql_f
import pyspark.sql.window as psql_wdw
import datetime
import sys

name_app = "Home cell"

dir_input = (
    "PATH_TO_INTERMEDIATE_TABLES_FOR_DEVICE_PRESENCE_BY_TIME_RANGE/"
    + "/tables_inter_homecell_update/"
)

dir_output_imsi_homecell = "PATH_TO_IMSI_REGISTER_WITH_HOMECELL"
dir_out_weighting = "PATH_TO_HOMECELL_N_IMSI_DETECTED_RESIDENTS_FOR_WEIGHTING"

spark = SparkSession.builder.appName(name_app).getOrCreate()

# Candidate cells
homecell1 = spark.read.parquet(dir_input + "2019_03_16/*")
homecell1 = homecell1.withColumn("period", psql_f.lit("2019_03_16"))
homecell2 = spark.read.parquet(dir_input + "2019_03_31/*")
homecell2 = homecell2.withColumn("period", psql_f.lit("2019_03_31"))
homecell3 = spark.read.parquet(dir_input + "2019_04_15/*")
homecell3 = homecell3.withColumn("period", psql_f.lit("2019_04_15"))
homecell4 = spark.read.parquet(dir_input + "2019_04_30/*")
homecell4 = homecell4.withColumn("period", psql_f.lit("2019_04_30"))
homecell5 = spark.read.parquet(dir_input + "2019_05_15/*")
homecell5 = homecell5.withColumn("period", psql_f.lit("2019_05_15"))
homecell6 = spark.read.parquet(dir_input + "2019_05_30/*")
homecell6 = homecell6.withColumn("period", psql_f.lit("2019_05_30"))

homecell = (
    homecell1.union(homecell2)
    .union(homecell3)
    .union(homecell4)
    .union(homecell5)
    .union(homecell6)
)

scope = (
    homecell.groupBy("imsi", "period")
    .agg(psql_f.first("distinct_days").alias("distinct_days"))
    .groupBy("imsi")
    .agg(psql_f.sum("distinct_days").alias("distinct_days"))
    .withColumn("in_scope", psql_f.col("distinct_days") > 30)
)

imsiscope = homecell.join(scope, on="imsi")

# Weighting scheme 1 - Home cells based on the maximum number of hours spent in total over the candidate cells (ranked 1 to 9 over by 15 days).
window1 = (
    psql_wdw.Window()
    .partitionBy("imsi")
    .orderBy(psql_f.desc("count_hours_tot_3months"))
)
# Weighting scheme 2 - Home cells based on the cell with a recurrent number of days ranked first or second.
window2 = (
    psql_wdw.Window()
    .partitionBy("imsi")
    .orderBy(psql_f.desc("distinct_rank_1or2_3months"))
)
# Weighting scheme 3 - Home cells based on the maximum number of hours spent in total over the candidate cells at night.
window3 = (
    psql_wdw.Window()
    .partitionBy("imsi", "in_time_range")
    .orderBy(psql_f.desc("count_hours_tot_3months_night"))
)
# Toy Weighting scheme 4 - Anchor points cells based on the maximum number of hours spent in total over the candidate cells during daytime.

w1 = (
    imsiscope.where((psql_f.col("cell_rank_all_period") < 10))
    .groupBy(
        "period", "in_scope", "imsi", "lac", "ci_sac"
    )  # collapse over time range for one obs imsi-cell-period 15jours
    .agg(psql_f.first(psql_f.col("count_hours_tot")).alias("count_hours_tot"))
    .groupBy(
        "in_scope", "imsi", "lac", "ci_sac"
    )  # collapse over time range and 15-days periods for one obs imsi-cell
    .agg(psql_f.sum("count_hours_tot").alias("count_hours_tot_3months"))
    .withColumn("rank", psql_f.row_number().over(window1))
    .where(psql_f.col("rank") == 1)  # Take the cell ranked first
    .select("in_scope", "imsi", "lac", "ci_sac")
)

w1.write.parquet(dir_output_imsi_homecell + "weighting1")

w1.groupBy("in_scope", "lac", "ci_sac").count().write.parquet(
    dir_out_weighting + "weighting1"
)


w34 = (
    imsiscope.groupBy("period", "in_scope", "in_time_range", "imsi", "lac", "ci_sac")
    .agg(
        psql_f.first(psql_f.col("in_time_range_count_hours")).alias(
            "in_time_range_count_hours"
        )
    )
    .groupBy("in_scope", "in_time_range", "imsi", "lac", "ci_sac")
    .agg(psql_f.sum("in_time_range_count_hours").alias("count_hours_tot_3months_night"))
    .withColumn("rank", psql_f.row_number().over(window3))
    .where(psql_f.col("rank") == 1)
    .select("in_scope", "in_time_range", "imsi", "lac", "ci_sac")
)

w34.write.parquet(dir_output_imsi_homecell + "weighting34")

w34.groupBy("in_scope", "in_time_range", "lac", "ci_sac").count().write.parquet(
    dir_out_weighting + "weighting34"
)


w2 = (
    imsiscope.where(psql_f.col("in_scope") == True)
    .groupBy("period", "imsi", "lac", "ci_sac")
    .agg(psql_f.first(psql_f.col("days_ranked_first")).alias("days_ranked_first"))
    .groupBy("imsi", "lac", "ci_sac")
    .agg(psql_f.sum("days_ranked_first").alias("distinct_rank_1or2_3months"))
    .withColumn("rank", psql_f.row_number().over(window2))
    .where(psql_f.col("rank") == 1)
    .select("imsi", "lac", "ci_sac")
)

w2.write.parquet(dir_output_imsi_homecell + "weighting2")

w2.groupBy("lac", "ci_sac").count().write.parquet(dir_out_weighting + "weighting2")
