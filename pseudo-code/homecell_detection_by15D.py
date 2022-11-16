"""
Residency characterisation (home cell algorithm)
Step 2 : Rank cells per device with a max-presence cell algorithm by two-weeks windows

This codes is run by looping over 15 days period due to computational constraints.
It outputs for each triplet (device, cell) the cells ranked 1 to 9 in distinct 
hours of presence over the full period, the cells ranked 1 to 2 in
terms of distinct hours of presence over the nights (or day) over the full period,
and the cells ranked 1 or 2 in terms of hours spent over one day, and the number
of days with this ranking

Shell loop:

sparkexec.sh contains spark-submit with appropriate parameters
then shell loop to launch every 15 days

now=$(date +"%Y/%m/%d" -d "2019/03/16")
end=$(date +"%Y/%m/%d" -d "2019/06/14")

while [[ "$now" < "$end" ]] 
do
    echo $now $(date)
    ./sparkexec.sh homecell_detection_by15D.py $now
    now=$(date +"%Y/%m/%d" -d "$now + 15 day"); 
done

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as psql_f
import pyspark.sql.window as psql_wdw
import datetime
import sys

if len(sys.argv) < 2:
    sys.exit("start day parameter missing")

day = sys.argv[1]
dt1 = datetime.datetime.strptime(day, "%Y/%m/%d")

dir_input = "PATH_TO_INTERMEDIATE_TABLES_FOR_DEVICE_PRESENCE_BY_TIME_RANGE/"
name_out_15 = dt1.strftime("%Y_%m_%d")

name_app = "Order presence cells by imsi on 15 days range"
spark = SparkSession.builder.appName(name_app).getOrCreate()

# Import counts of hourly events per user (all dates - maximum of dates here)
days = ",".join([(dt1 + datetime.timedelta(t)).strftime("%Y_%m_%d") for t in range(15)])
print("reading input data for days", days)
daily_per_period_records = spark.read.parquet(
    dir_input + "tables_inter_by_period/{%s}" % days
)

# Define windows by imsi, by imsi and time range, etc..
windowImsi = psql_wdw.Window().partitionBy("imsi")
windowImsiTimerange = psql_wdw.Window().partitionBy("imsi", "in_time_range")
windowImsiCell = psql_wdw.Window().partitionBy("imsi", "lac", "ci_sac")
windowImsiDateCell = psql_wdw.Window().partitionBy("imsi", "day", "lac", "ci_sac")
windowImsiTimerangeCell = psql_wdw.Window().partitionBy(
    "imsi", "lac", "ci_sac", "in_time_range"
)


# Sorted windows for ranking cells according to various criteria
windowAllPeriod = (
    psql_wdw.Window().partitionBy("imsi").orderBy(psql_f.desc("count_hours_tot"))
)
windowDate = (
    psql_wdw.Window()
    .partitionBy("imsi", "day")
    .orderBy(psql_f.desc("count_hours_date"))
)
windowAllPeriodNight = (
    psql_wdw.Window()
    .partitionBy("imsi", "in_time_range")
    .orderBy(psql_f.desc("count_hours_tot_night"))
)
windowDistinctDay = (
    psql_wdw.Window()
    .partitionBy("imsi")
    .orderBy(psql_f.desc("distinct_days_ranked_first"))
)


homecell = (
    daily_per_period_records.withColumn(
        "count_hours_date", psql_f.sum("count_hours").over(windowImsiDateCell)
    )
    .withColumn("count_hours_tot", psql_f.sum("count_hours").over(windowImsiCell))
    .withColumn(
        "count_hours_tot_night", psql_f.sum("count_hours").over(windowImsiTimerangeCell)
    )
    .withColumn(
        "distinct_days", psql_f.size(psql_f.collect_set("day").over(windowImsi))
    )
    .withColumn(
        "distinct_days_or_nights",
        psql_f.size(psql_f.collect_set("day").over(windowImsiTimerange)),
    )
    .withColumn("cell_rank_by_day", psql_f.rank().over(windowDate))
    .withColumn("cell_rank_all_period", psql_f.rank().over(windowAllPeriod))
    .withColumn("cell_rank_all_period_night", psql_f.rank().over(windowAllPeriodNight))
    .where(
        (psql_f.col("cell_rank_by_day") < 3)
        | (psql_f.col("cell_rank_all_period") < 10)
        | (psql_f.col("cell_rank_all_period_night") < 3)
    )
    .groupBy(
        "imsi",
        "distinct_days",
        "distinct_days_or_nights",
        "lac",
        "ci_sac",
        "in_time_range",
    )
    .agg(  # Across dates
        psql_f.min(psql_f.col("cell_rank_all_period_night")).alias(
            "cell_rank_all_period_in_time_range"
        ),  # For a given cell, this rank is unique across dates: we take the min
        psql_f.min(psql_f.col("cell_rank_all_period")).alias(
            "cell_rank_all_period"
        ),  # For a given cell, this rank is unique across dates: we take the min
        psql_f.sum(
            psql_f.when(psql_f.col("cell_rank_by_day") == 1, 1).otherwise(0)
        ).alias(
            "days_ranked_first"
        ),  # Keep the number of dates the cell is ranked first
        psql_f.sum(
            psql_f.when(psql_f.col("cell_rank_by_day") == 2, 1).otherwise(0)
        ).alias(
            "days_ranked_second"
        ),  # eep the number of dates the cell is ranked second
        psql_f.first(psql_f.col("count_hours_tot")).alias(
            "count_hours_tot"
        ),  # For a given cell, this count is unique across dates: we take the first to keep this count
        psql_f.first(psql_f.col("count_hours_tot_night")).alias(
            "in_time_range_count_hours"
        ),  # For a given cell, time_range, this count is unique across dates: we take the first to keep this count
    )
)

homecell.write.parquet(dir_input + "tables_inter_homecell_update/" + name_out_15)
