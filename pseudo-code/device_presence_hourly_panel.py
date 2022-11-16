"""
Device presence panel module
"""

from import_raw_signaling_data import import_data


import sys
import datetime
import pyspark.sql.functions as psql_f
import pyspark.sql.types as psql_t
import pyspark.sql.window as psql_wdw


dir_topo = "PATH_TO_RADIO_CELLS_REGISTRY"
dir_imsis = "PATH_TO_IMSI_KNOWN_AS_MOBILE_PHONE_REGISTRY"
dir_out = "PATH_TO_INTERMEDIATE_TABLES_FOR_DEVICE_PRESENCE_PANEL"


def hourly_imsi_cell(spark, data, psql_f, day, dir_out):

    hourly_indiv_x_cell = data.groupBy(
        "imsi", "hour", "lac", "ci_sac"
    ).count()  # hour format: MM-dd HH

    # A reference time scale
    DeltaH = 1
    H0 = 5
    daydt = datetime.datetime.strptime(day, "%Y/%m/%d")
    refTimes = [
        (int(d.strftime("%s")), d.strftime("%m-%d %H"))
        for d in (
            daydt + datetime.timedelta(hours=i) for i in range(H0, H0 + 24, DeltaH)
        )
    ]
    refTimes = sc.parallelize(refTimes)
    schemaT = psql_t.StructType(
        [
            psql_t.StructField("ref_hourInt", psql_t.IntegerType(), True),
            psql_t.StructField("ref_hour", psql_t.StringType(), True),
        ]
    )
    refTimes = spark.createDataFrame(refTimes, schemaT)

    # Attribute to each imsi, at each t in this reference time scale, all radio cell x hour it was connected to in the data subset
    phone_imsis = (
        spark.read.parquet(dir_imsis)
        .select("imsi")
        .where(psql_f.col("imsi").substr(1, 5) == "20801")
    )  # Filter on Orange client only
    imsi_refhour = phone_imsis.crossJoin(psql_f.broadcast(refTimes)).join(
        hourly_indiv_x_cell, on="imsi"
    )
    hour_ts = psql_f.unix_timestamp(
        psql_f.concat(psql_f.lit("2019-"), psql_f.col("hour")), "yyyy-MM-dd HH"
    )

    # Difference between the reference hour and the observed hour
    imsi_refhour = imsi_refhour.withColumn(
        "diff_hour",
        psql_f.abs((hour_ts - psql_f.col("ref_hourInt")) / 3600).cast("int"),
    )

    # Window for Sorting
    # - On observed hour proximity with reference times
    # - Conditional on the observed hour, rank cells with events' count.
    w = (
        psql_wdw.Window()
        .partitionBy("imsi", "ref_hour")
        .orderBy(psql_f.col("diff_hour"), psql_f.col("count").desc())
    )

    # Keep only one cell per hour
    imsi_refhour = (
        imsi_refhour.withColumn("rank", psql_f.row_number().over(w))
        .where(
            psql_f.col("rank") == 1
        )  # Keep imsi-refhour row with the least distant observed hour + the cell with the max of events.
        .drop("rank")
        .select(
            "imsi",
            psql_f.col("hour").alias("hour_obs"),
            "ref_hour",
            "lac",
            "ci_sac",
            psql_f.col("count").alias("count_events"),
            "diff_hour",
        )
    )

    print("Exporting results")

    imsi_refhour.repartition(1).write.parquet(dir_out + "temp_tables/" + day)

    print("Exporting done")


#### Application spark-------------------------------------------------------------

if __name__ == "__main__":

    from pyspark.sql import SparkSession

    name_app = "Device Presence Panel"
    spark = SparkSession.builder.appName(name_app).getOrCreate()

    print("Data for a day: import", datetime.datetime.now())

    data = import_data(
        spark=spark, psql_f=psql_f, day=day, dir_topo=dir_topo, dir_imsis=dir_imsis
    )

    print("Data for a day: imported", datetime.datetime.now())

    hourly_imsi_cell(
        spark=spark,
        data=data,
        psql_f=psql_f,
        psql_t=psql_t,
        psql_wdw=psql_wdw,
        day=day,
        dir_out=dir_out,
    )
