"""
Import a day of raw signaling data and apply all purpose filters
"""

import sys
import datetime
import pyspark.sql.functions as psql_f


dir_topo = "PATH_TO_RADIO_CELLS_REGISTRY"
dir_imsis = "PATH_TO_IMSI_KNOWN_AS_MOBILE_PHONE_REGISTRY"
dir_data = "PATH_TO_RAW_SIGNALING_DATA"


def import_data(
    spark,
    psql_f,
    day,
    dir_topo,
    dir_imsis,
    filterOrange=True,
    filterPhone=True,
    filterRoamer=True,
    technos=["2G", "3G", "4G"],
):

    # Filter for only IMSIS using a "phone" as defined in Orange TAC codes list
    if filterPhone:
        phone_imsis = spark.read.parquet(dir_imsis).select("imsi")

    topo = spark.read.csv(dir_topo, header="True", sep=";").filter(
        "'{}' between min_dt and max_dt".format(day)
    )

    # Data import start hour
    day_J1 = datetime.datetime.strptime(day, "%Y/%m/%d")
    # Data import end hour
    day_J2 = day_J1 + datetime.timedelta(hours=29) - datetime.timedelta(seconds=1)
    # Data from midnight to 29hours later

    data_day = None

    # Import 2G data if specified
    if "2G" in technos:
        print("Import 2G data")

        #
        ts_2G = (
            psql_f.unix_timestamp(
                psql_f.concat(
                    psql_f.col("event_day"), psql_f.lit(" "), psql_f.col("event_time")
                ),
                "dd/MM/yyyy HH:mm:ss",
            )
            .cast("timestamp")
            .alias("event_timestamp")
        )
        df_2g_J1 = spark.read.parquet(
            dir_data + "A/" + day_J1.strftime("%Y/%m/%d")
        ).select(
            [psql_f.col("IMSI").alias("imsi"), "LAC", "cell_id", ts_2G]
        )
        df_2g_J1 = df_2g_J1.filter(df_2g_J1.event_timestamp.between(day_J1, day_J2))
        df_2g_J2 = spark.read.parquet(
            dir_data + "A/" + day_J2.strftime("%Y/%m/%d")
        ).select(
            [psql_f.col("IMSI").alias("imsi"), "LAC", "cell_id", ts_2G]
        )
        df_2g_J2 = df_2g_J2.filter(df_2g_J2.event_timestamp.between(day_J1, day_J2))

        df_2g = df_2g_J1.union(df_2g_J2)

        if filterRoamer:
            df_2g = df_2g.where(psql_f.col("imsi").substr(1, 3) == "208")
        if filterOrange:
            df_2g = df_2g.where(psql_f.col("imsi").substr(1, 5) == "20801")
        if filterPhone:
            df_2g = df_2g.join(phone_imsis, "imsi")

        # Extract month, day and hour from timestamp
        df_2g = df_2g.withColumn(
            "hour", psql_f.date_format("event_timestamp", "MM-dd HH")
        )

        # Subset to rows without missing values 
        # (necessay to join with radio cell register)
        df_2g = df_2g.where(
            psql_f.col("LAC").isNotNull()
            & psql_f.col("cell_id").isNotNull()
        )

        # Keep relevant columns and rename them as 4G files
        df_2g = df_2g.selectExpr(
            "imsi",
            "cast(LAC as int) lac",
            "cast(cell_id as int) ci_sac",
            "cast('2' as int) techno",
            "hour as hour",
        )

        # Innerjoin (filter) with radio cell registry
        topo_2g = topo.filter("TECHNO='2G'").select(
            topo.CI.alias("ci_sac"), topo.LAC.alias("lac")
        )
        df_2g = df_2g.join(psql_f.broadcast(topo_2g), ["ci_sac", "lac"])

        data_day = df_2g

    # Import 3G data if specified
    if "3G" in technos:
        print("Import 3G data")

        ts_3g = (
            psql_f.unix_timestamp(psql_f.col("event_time"), "dd/MM/yyyy HH:mm:ss")
            .cast("timestamp")
            .alias("event_timestamp")
        )
        df_3g_J1 = spark.read.parquet(
            dir_data + "IU/" + day_J1.strftime("%Y/%m/%d")
        ).select([psql_f.col("IMSI").alias("imsi"), "lac", "sac", ts_3g])
        df_3g_J1 = df_3g_J1.filter(df_3g_J1.event_timestamp.between(day_J1, day_J2))
        df_3g_J2 = spark.read.parquet(
            dir_data + "IU/" + day_J2.strftime("%Y/%m/%d")
        ).select([psql_f.col("IMSI").alias("imsi"), "lac", "sac", ts_3g])
        df_3g_J2 = df_3g_J2.filter(df_3g_J2.event_timestamp.between(day_J1, day_J2))

        df_3g = df_3g_J1.union(df_3g_J2)

        if filterRoamer:
            df_3g = df_3g.where(psql_f.col("imsi").substr(1, 3) == "208")
        if filterOrange:
            df_3g = df_3g.where(psql_f.col("imsi").substr(1, 5) == "20801")
        if filterPhone:
            df_3g = df_3g.join(phone_imsis, "imsi")

        # Extract month, day and hour from timestamp
        df_3g = df_3g.withColumn(
            "hour", psql_f.date_format("event_timestamp", "MM-dd HH")
        )

        # Subset to rows without missing values 
        # (necessay to join with radio cell register)
        df_3g = df_3g.where(
            psql_f.col("lac").isNotNull() & psql_f.col("sac").isNotNull()
        )

        # Keep relevant columns and rename them as 4G files
        df_3g = df_3g.selectExpr(
            "imsi",
            "cast(lac as int) lac",
            "cast(sac as int) ci_sac",
            "cast('3' as int) techno",
            "hour as hour",
        )

        topo_3g = topo.filter("TECHNO='3G'").select(
            topo.CI.alias("ci_sac"), topo.LAC.alias("lac")
        )
        df_3g = df_3g.join(psql_f.broadcast(topo_3g), ["ci_sac", "lac"])

        if data_day:
            data_day = data_day.union(df_3g)
        else:
            data_day = df_3g

    # Import 4G data if specified
    if "4G" in technos:
        print("Import 4G data")

        ts_4g = psql_f.from_unixtime("event_date_utc").alias("event_timestamp")
        df_4g_J1 = spark.read.parquet(
            dir_data + "S1C/" + day_J1.strftime("%Y/%m/%d")
        ).select(["imsi", "enodeb_id", "cell_id", ts_4g])
        df_4g_J1 = df_4g_J1.filter(df_4g_J1.event_timestamp.between(day_J1, day_J2))
        df_4g_J2 = spark.read.parquet(
            dir_data + "S1C/" + day_J2.strftime("%Y/%m/%d")
        ).select(["imsi", "enodeb_id", "cell_id", ts_4g])
        df_4g_J2 = df_4g_J2.filter(df_4g_J2.event_timestamp.between(day_J1, day_J2))

        df_4g = df_4g_J1.union(df_4g_J2)

        if filterRoamer:
            df_4g = df_4g.where(psql_f.col("imsi").substr(1, 3) == "208")
        if filterOrange:
            df_4g = df_4g.where(psql_f.col("imsi").substr(1, 5) == "20801")
        if filterPhone:
            df_4g = df_4g.join(phone_imsis, "imsi")

        # Extract month, day and hour from timestamp
        df_4g = df_4g.withColumn(
            "hour", psql_f.date_format("event_timestamp", "MM-dd HH")
        )

        # Subset to rows without missing values for enodeb_id and cell_id
        # (both necessay to compute complete CID)
        # enodeb_id is almost never missing, but cell_id is in about 40% of events,
        # mainly when type = 9 ("S1 Release")
        df_4g = df_4g.where(
            psql_f.col("enodeb_id").isNotNull() & psql_f.col("cell_id").isNotNull()
        )
        # Creation of ci_sac column for merging : ci_sac = 256*enodeb_id + cell_id
        # Verification using : https://www.cellmapper.net/enbid?lang=fr
        # We also create lac column to have consistent columns with other technologies
        # lac is always equal to 1 for 4G cells
        df_4g = df_4g.withColumn(
            "ci_sac", 256 * psql_f.col("enodeb_id") + psql_f.col("cell_id")
        ).withColumn("lac", psql_f.lit(1))
        #
        # Keep relevant columns
        df_4g = df_4g.selectExpr(
            "imsi",
            "cast(lac as int) lac",
            "cast(ci_sac as int) ci_sac",
            "cast('4' as int) techno",
            "hour as hour",
        )

        topo_4g = topo.filter("TECHNO='4G'").select(
            topo.CI.alias("ci_sac"), topo.LAC.alias("lac")
        )
        df_4g = df_4g.join(psql_f.broadcast(topo_4g), ["ci_sac", "lac"])

        if data_day:
            data_day = data_day.union(df_4g)
        else:
            data_day = df_4g

    # Drop missing/null values
    data_day = data_day.dropna(how="any")

    return data_day
