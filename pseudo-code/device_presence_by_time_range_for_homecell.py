"""
Residency characterisation (home cell algorithm)
Step 1 : Reduce dimension of raw data to device x presence cell x date entries
"""

import sys
import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as psql_f

from import_raw_signaling_data import import_data

# le jour de données à charger au format YYYY/MM/DD
if len(sys.argv) < 2:
    sys.exit("paramètre manquant : le jour de données à charger au format YYYY/MM/DD")

day = sys.argv[1]

dir_topo = "PATH_TO_RADIO_CELLS_REGISTRY"
dir_imsis = "PATH_TO_IMSI_KNOWN_AS_MOBILE_PHONE_REGISTRY"
dir_out = "PATH_TO_INTERMEDIATE_TABLES_FOR_DEVICE_PRESENCE_BY_TIME_RANGE/"


def in_timerange(psql_f, df, timerange, format="MM-dd HH"):
    """
    Create a variable distinguish 2 periods of the day based on one given time range, in mobile phone data to a given timerange
    Parameters
    ----------
    psql_f: pyspark.sql.functions
            Module passed as dependency
    df: SparkDataFrame
            Mobile phone data
    timerange: tuple
            Timerange to subset to (including brackets). Example : (21,7) for 21PM-7AM


    return: SparkDataFrame
    """
    if format == "MM-dd HH":
        if timerange[1] >= timerange[0]:
            df = df.withColumn(
                "in_time_range",
                (psql_f.col("hour").substr(7, 8).between(timerange[0], timerange[1])),
            )
        else:
            df = df.withColumn(
                "in_time_range",
                (psql_f.col("hour").substr(7, 8).between(timerange[0], 24))
                | (psql_f.col("hour").substr(7, 8).between(0, timerange[1])),
            )
    else:
        if timerange[1] >= timerange[0]:
            df = df.withColumn(
                "in_time_range",
                (psql_f.col("hour").between(timerange[0], timerange[1])),
            )
        else:
            df = df.withColumn(
                "in_time_range",
                (psql_f.col("hour").between(timerange[0], 24))
                | (psql_f.col("hour").between(0, timerange[1])),
            )

    return df


name_app = "Device presence by time range"
spark = SparkSession.builder.appName(name_app).getOrCreate()

data = import_data(
    spark=spark, psql_f=psql_f, day=day, dir_topo=dir_topo, dir_imsis=dir_imsis
)

print("Statistics for day " + day, datetime.datetime.now())

agg_records = data.groupBy("imsi", "lac", "ci_sac", "hour").count()

agg_records = in_timerange(psql_f, agg_records, (9, 17))

(
    agg_records.groupBy("in_time_range", "imsi", "lac", "ci_sac")
    .agg(
        psql_f.sum("count").alias("count_events"),
        psql_f.countDistinct("hour").alias("count_hours"),
    )
    .withColumn("day", psql_f.lit(day).substr(6, 5))
    .write.parquet(dir_out + "tables_inter_by_period/" + day.replace("/", "_"))
)

print("Statistics for day " + day + " done", datetime.datetime.now())
