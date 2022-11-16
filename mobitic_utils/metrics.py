import os
from sqlalchemy import create_engine
import pandas as pd
import geopandas as gpd
import datetime as dt
from mobitic_utils.utils_s3 import write_pandas_s3, get_pandas_s3
from mobitic_utils.constants import (
    POSTGRESQL_USER,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_HOST,
    POSTGRESQL_PORT,
)

engine = create_engine(
    f"postgresql://{POSTGRESQL_USER}:{POSTGRESQL_PASSWORD}\
@{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/defaultdb"
)


def get_metrics():
    """
    Compare, Filosofi population data (2016) and our latest Dynamic population data hour by hour over 16/03/2019 to 31/03/2019, grouping by type of "Aire d'attraction des villes"
    """

    query = """WITH data as (
                SELECT p.%s as estimation,
                        COALESCE(p.filosofi,0) as truth,
			  		ROW_NUMBER() OVER (ORDER BY p.filosofi DESC) AS Rank_truth,
			  		ROW_NUMBER() OVER (ORDER BY p.%s DESC) AS Rank_estimation,
                        aav.LIBTAAV as segmentation
                FROM total_population p, grille g, (SELECT ST_UNION(a.geometry) as geometry, a."LIBTAAV" as LIBTAAV FROM area_attraction a GROUP BY a."LIBTAAV") as aav
                WHERE week = %d
                AND g."x-y"=p."x-y"
                AND ST_INTERSECTS(aav.geometry, g.geometry))
            SELECT 
                AVG(ABS(truth-estimation)), 
                SQRT(AVG((truth-estimation)^2)), 
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY abs(truth-estimation)), 
                CORR(truth, estimation),
                1-0.5*SUM(ABS(estimation-truth))/SUM(truth) as alloc_accuracy,
                CORR(Rank_truth, Rank_estimation),
                segmentation
            FROM data
            GROUP BY segmentation"""

    l = []

    date = dt.datetime(2019, 3, 18, 0)  # week 12, day 0, hour 0

    with engine.connect() as connection:
        for week in range(12, 14):  # Only two weeks
            for day in range(0, 7):
                for hour in range(0, 24, 3):
                    res = connection.execute(
                        query % (f'"{day}-{hour}"', f'"{day}-{hour}"', week)
                    ).fetchall()
                    l += [list(x) + [date] for x in res]
                    date += dt.timedelta(hours=3)
    return pd.DataFrame(
        l,
        columns=[
            "MAE",
            "RMSE",
            "median_mae",
            "correlation",
            "alloc_accuracy",
            "rank_cor",
            "segmentation",
            "date",
        ],
    )


def save_metrics():
    """
    Compare and save, Filosofi population data and our latest Dynamic population data hour by hour over 16/03/2019 to 31/03/2019, grouping by type of "Aire d'attraction des villes"
    """

    df = get_metrics()
    # write_pandas_s3(df, Key="cancan/tao/metrics.csv")
    write_pandas_s3(df, Key="cancan/revision/metrics.csv")
    return "Done!"


def import_metrics():
    return get_pandas_s3(Key="cancan/revision/metrics.csv")


def get_metrics_JRC():
    """
    Compare, JRC population data from march 2011 (day and night) and and our latest Dynamic population data hour by hour over 16/03/2019 to 31/03/2019, grouping by type of "Aire d'attraction des villes"
    """

    query = """WITH data as (
                SELECT p.%s as estimation_day,
                        p.%s as estimation_night,
                        jd.population as truth_day,
                        jn.population as truth_night,
                        COALESCE(p.filosofi,0) as residents,
						aav.LIBTAAV as segmentation
                FROM total_population p, jrc jd, jrc jn, grille g, (SELECT ST_UNION(a.geometry) as geometry, a."LIBTAAV" as LIBTAAV FROM area_attraction a GROUP BY a."LIBTAAV") as aav
                WHERE p.week=%d
                AND ST_INTERSECTS(aav.geometry, g.geometry)
                AND g."x-y"=p."x-y"
                AND p."x-y"=jd."x-y" 
                AND jd."x-y"=jn."x-y"
                AND jd.day_night='D' AND jn.day_night='N'
                AND jn.month=%d AND jd.month= %d)
            SELECT 
                AVG(ABS(truth_day-estimation_day)), 
                SQRT(AVG((truth_day-estimation_day)^2)), 
                CORR(truth_day, estimation_day),
                1-0.5*SUM(ABS(estimation_day-truth_day))/SUM(truth_day),
                AVG(ABS(truth_night-estimation_night)), 
                SQRT(AVG((truth_night-estimation_night)^2)), 
                CORR(truth_night, estimation_night),
                1-0.5*SUM(ABS(estimation_night-truth_night))/SUM(truth_night),
                AVG(ABS(truth_day-residents)), 
                SQRT(AVG((truth_day-residents)^2)), 
                CORR(truth_day, residents),
                1-0.5*SUM(ABS(residents-truth_day))/SUM(residents),
                AVG(ABS(truth_night-residents)), 
                SQRT(AVG((truth_night-residents)^2)), 
                CORR(truth_night, residents),
                1-0.5*SUM(ABS(residents-truth_night))/SUM(residents),
                segmentation
            FROM data
			GROUP BY segmentation
"""

    l = []
    date = dt.datetime(2019, 3, 18, 0)
    hour_day = 15
    hour_night = 3
    month_jrc = 3

    with engine.connect() as connection:
        for week in range(12, 14):
            for day in range(0, 7):
                res = connection.execute(
                    query
                    % (
                        f'"{day}-{hour_day}"',
                        f'"{day}-{hour_night}"',
                        week,
                        month_jrc,
                        month_jrc,
                    )
                ).fetchall()
                l += [list(x) + [date] for x in res]
                date += dt.timedelta(hours=24)
    return pd.DataFrame(
        l,
        columns=[
            "MAE_D",
            "RMSE_D",
            "correlation_D",
            "alloc_accuracy_D",
            "MAE_N",
            "RMSE_N",
            "correlation_N",
            "alloc_accuracy_N",
            "MAE_D_JRC_RES",
            "RMSE_D_JRC_RES",
            "correlation_D_JRC_RES",
            "alloc_accuracy_D_JRC_RES",
            "MAE_N_JRC_RES",
            "RMSE_N_JRC_RES",
            "correlation_N_JRC_RES",
            "alloc_accuracy_N_JRC_RES",
            "area_attraction",
            "date",
        ],
    )


def get_allocation(attraction_area=False):
    """
    Compare Filosofi data from march 2016 and and our latest Dynamic population data hour by hour over 16/03/2019 to 31/03/2019, grouping (or not) by type of "Aire d'attraction des villes"

    Metrics:
    Allocation accuracy
    Correlation
    """

    if attraction_area == True:
        query = """WITH data as (SELECT p.%s * ST_AREA(g.geometry)/1000000 as population,
                                        COALESCE(p.filosofi, 0)* ST_AREA(g.geometry)/1000000 as filosofi, 
                                        ar."LIBTAAV" as segmentation
                                FROM total_population as p, grille as g, area_attraction as ar
                                WHERE g."x-y" = p."x-y" AND
                                  ST_intersects(g.geometry, ar.geometry) AND
                                  p.week=%d)
                    SELECT 
                        1 - 0.5*SUM(ABS(filosofi-population))/SUM(filosofi) as alloc_accuracy,
                        CORR(filosofi,population) as correlation,
                        segmentation
                    FROM data 
                    GROUP BY segmentation"""
        col_names = ["ALLOC_ACCURACY", "CORRELATION", "LIBTAAV", "date"]

    else:
        query = """WITH data as (SELECT p.%s* ST_AREA(g.geometry)/1000000 as population, 
                                        COALESCE(p.filosofi, 0)* ST_AREA(g.geometry)/1000000 as filosofi
                FROM total_population as p, grille as g
                WHERE p."x-y" = g."x-y"
                AND p.week=%d)
                SELECT 
                    1 - 0.5*SUM(ABS(filosofi-population))/SUM(filosofi) as alloc_accuracy,
                    CORR(filosofi,population) as correlation
                FROM data """

        col_names = ["ALLOC_ACCURACY", "CORRELATION", "date"]

    l = []
    date = dt.datetime(2019, 3, 11, 0)

    with engine.connect() as connection:
        for week in range(11, 13):
            for day in range(0, 7):
                for hour in range(0, 24):
                    res = connection.execute(
                        query % (f'"{day}-{hour}"', week)
                    ).fetchall()
                    l += [list(x) + [date] for x in res]
                    date += dt.timedelta(hours=1)

    return pd.DataFrame(
        l,
        columns=col_names,
    )


def get_cosine():
    """
    Compare Filosofi data from march 2016 and and our latest Dynamic population (and device) data hour by hour over 16/03/2019 to 31/03/2019, grouping (or not) by type of "Aire d'attraction des villes"

    Metrics:
    Cosine distance
    """

    query = """WITH data as (SELECT p.%s as population,
			  		                s.%s as mobile,
			  		COALESCE(p.filosofi, 0) as filosofi
                FROM total_population as p, active_devices as s
			  	WHERE s."x-y" = p."x-y" AND
			  		  p.week=%d AND
                      s.week = p.week)
                SELECT 
                    SUM(filosofi*population)/(SQRT(SUM(POWER(filosofi,2)))*SQRT(SUM(POWER(population,2)))) as cosine_poppres,
                    SUM(filosofi*mobile)/(SQRT(SUM(POWER(filosofi,2)))*SQRT(SUM(POWER(mobile,2)))) as cosine_mobile
                FROM data """

    col_names = ["COS_POPPRES", "COS_MOBILE", "date"]

    l = []
    date = dt.datetime(2019, 3, 11, 0)

    with engine.connect() as connection:
        for week in range(11, 13):
            for day in range(0, 7):
                for hour in range(0, 24):
                    res = connection.execute(
                        query % (f'"{day}-{hour}"', f'"{day}-{hour}"', week)
                    ).fetchall()
                    l += [list(x) + [date] for x in res]
                    date += dt.timedelta(hours=1)

    return pd.DataFrame(
        l,
        columns=col_names,
    )


def save_metrics_JRC():
    df = get_metrics_JRC()
    # write_pandas_s3(df, Key="cancan/tao/metrics_JRC.csv")
    write_pandas_s3(df, Key="cancan/revision/metrics_JRC.csv")
    return "Done!"


def import_metrics_JRC():
    return get_pandas_s3(Key="cancan/revision/metrics_JRC.csv")


def save_metrics_alloc_accuracy():
    df = get_allocation()
    df_a = get_allocation(attraction_area=True)
    # write_pandas_s3(df, Key="cancan/metrics/metrics_alloc_accuracy_counts.csv")
    # write_pandas_s3(df_a, Key="cancan/metrics/metrics_alloc_accuracy_counts_by_AA.csv")
    write_pandas_s3(df, Key="cancan/revision/metrics_alloc_accuracy_counts.csv")
    write_pandas_s3(df_a, Key="cancan/revision/metrics_alloc_accuracy_counts_by_AA.csv")
    return "Done!"


def save_metrics_cosine():
    df = get_cosine()
    # write_pandas_s3(df, Key="cancan/metrics/metrics_cosine_active_pop_vs_filo.csv")
    write_pandas_s3(df, Key="cancan/revision/metrics_cosine_active_pop_vs_filo.csv")
    return "Done!"


def get_autocorrelation(attraction_area=False):

    if attraction_area == True:
        query = """WITH data as (SELECT p.%s as population_0, 
                                        p.%s as population_1,
                                        ar."LIBTAAV" as segmentation
                                FROM total_population as p, grille as g, area_attraction as ar
                                WHERE g."x-y" = p."x-y" AND
                                  ST_intersects(g.geometry, ar.geometry) AND
                                  p.week=%d)
                    SELECT 
                        CORR(population_1,population_0) as correlation,
                        segmentation
                    FROM data 
                    GROUP BY segmentation"""
        col_names = ["CORRELATION", "LIBTAAV", "date"]

    else:
        query = """WITH data as (SELECT p.%s as population_0, 
                                        p.%s as population_1
                FROM total_population as p
                WHERE p.week=%d)
                SELECT 
                    CORR(population_1,population_0) as correlation
                FROM data """

        col_names = ["CORRELATION", "date"]

    l = []
    date = dt.datetime(2019, 3, 11, 0)

    with engine.connect() as connection:
        for week in range(11, 13):
            day_ref = 0
            hour_ref = 0
            for day in range(0, 7):
                for hour in range(0, 24):
                    res = connection.execute(
                        query % (f'"{day}-{hour}"', f'"{day_ref}-{hour_ref}"', week)
                    ).fetchall()
                    l += [list(x) + [date] for x in res]
                    date += dt.timedelta(hours=1)

    return pd.DataFrame(
        l,
        columns=col_names,
    )


def save_metrics_autocorrelation():
    df = get_autocorrelation()
    df_a = get_autocorrelation(attraction_area=True)
    write_pandas_s3(df, Key="cancan/metrics/metrics_autocorrelation.csv")
    write_pandas_s3(df_a, Key="cancan/metrics/metrics_autocorrelation_AA.csv")
    return "Done!"


def get_metrics_from_all_tables(query_metrics, metrics_name):
    """
    Only the week where device and dynamic population are available jointly is considered
    Device density is rescaled globally by an hourly factor so that the sum of active devices equals filosofi population

    """
    query = """WITH data as (SELECT p.%s as pop_night,
			  		p.%s as pop_day,
                    ad.%s*(SELECT SUM(p.filosofi*ST_AREA(g.geometry)) FROM total_population p, grille g WHERE p."x-y"=g."x-y" AND p.week=%d)/(SELECT SUM(ad.%s*ST_AREA(g.geometry)) FROM active_devices ad, grille g WHERE ad."x-y"=g."x-y" AND ad.week=%d) as dev_act_night,
                    ad.%s*(SELECT SUM(p.filosofi*ST_AREA(g.geometry)) FROM total_population p, grille g WHERE p."x-y"=g."x-y" AND p.week=%d)/(SELECT SUM(ad.%s*ST_AREA(g.geometry)) FROM active_devices ad, grille g WHERE ad."x-y"=g."x-y" AND ad.week=%d) as dev_act_day,
                    d.%s*(SELECT SUM(p.filosofi*ST_AREA(g.geometry)) FROM total_population p, grille g WHERE p."x-y"=g."x-y" AND p.week=%d)/(SELECT SUM(d.%s*ST_AREA(g.geometry)) FROM total_devices d, grille g WHERE d."x-y"=g."x-y" AND d.week=%d) as dev_night,
                    d.%s*(SELECT SUM(p.filosofi*ST_AREA(g.geometry)) FROM total_population p, grille g WHERE p."x-y"=g."x-y" AND p.week=%d)/(SELECT SUM(d.%s*ST_AREA(g.geometry)) FROM total_devices d, grille g WHERE d."x-y"=g."x-y" AND d.week=%d) as dev_day,
                    jd.population as jrc_day,
                    jn.population as jrc_night,
					COALESCE(p.filosofi,0) as residents,
			  		ROW_NUMBER() OVER (ORDER BY p.%s DESC) AS Rank_pop_night,
			  		ROW_NUMBER() OVER (ORDER BY p.%s DESC) AS Rank_pop_day,
			  		ROW_NUMBER() OVER (ORDER BY d.%s DESC) AS Rank_dev_night,
			  		ROW_NUMBER() OVER (ORDER BY d.%s DESC) AS Rank_dev_day,
			  		ROW_NUMBER() OVER (ORDER BY COALESCE(p.filosofi,0) DESC) AS Rank_residents,
			  		ROW_NUMBER() OVER (ORDER BY jd.population DESC) AS Rank_jrc_day,
			  		ROW_NUMBER() OVER (ORDER BY jn.population DESC) AS Rank_jrc_night
			  FROM total_population p, active_devices ad, total_devices d, jrc jd, jrc jn
                WHERE p.week=%d AND jd.day_night='D' AND jn.day_night='N'
                AND jn.month=%d AND jd.month= %d
                AND d.week=%d
                AND ad.week=%d
                AND p."x-y"=jd."x-y" 
                AND p."x-y"=d."x-y" 
			    AND jd."x-y"=jn."x-y"
                AND d."x-y"=ad."x-y"
                )
        SELECT 	
                %s	
            FROM data """

    l = []
    date = dt.datetime(2019, 3, 18, 0)
    hour_day = 15
    hour_night = 3
    month_jrc = 3
    with engine.connect() as connection:
        for week in range(12, 14):
            for day in range(0, 5):  # Only Working Days.
                res = connection.execute(
                    query
                    % (
                        f'"{day}-{hour_night}"',
                        f'"{day}-{hour_day}"',
                        f'"{day}-{hour_night}"',
                        week,
                        f'"{day}-{hour_night}"',
                        week,
                        f'"{day}-{hour_day}"',
                        week,
                        f'"{day}-{hour_day}"',
                        week,
                        f'"{day}-{hour_night}"',
                        week,
                        f'"{day}-{hour_night}"',
                        week,
                        f'"{day}-{hour_day}"',
                        week,
                        f'"{day}-{hour_day}"',
                        week,
                        f'"{day}-{hour_night}"',
                        f'"{day}-{hour_day}"',
                        f'"{day}-{hour_night}"',
                        f'"{day}-{hour_day}"',
                        week,
                        month_jrc,
                        month_jrc,
                        week,
                        week,
                        query_metrics,
                    )
                ).fetchall()
                l += [list(x) + [day] + [week] + [query_metrics] for x in res]
    return pd.DataFrame(l, columns=["value", "day", "week", "query"])


def get_summary_metrics():
    # Compare all sources to residents.
    var_list = [
        "pop_day",
        "pop_night",
        "dev_day",
        "dev_night",
        "dev_act_day",
        "dev_act_night",
        "jrc_day",
        "jrc_night",
    ]
    ref = "residents"
    metrics = pd.concat(
        [
            pd.concat(
                [
                    get_metrics_from_all_tables("CORR(%s, %s)" % (var, ref), "cor")
                    for var in var_list
                ]
            ),
            pd.concat(
                [
                    get_metrics_from_all_tables(
                        "CORR(Rank_%s, Rank_%s)" % (var, ref), "rank_cor"
                    )
                    for var in var_list
                ]
            ),
            get_metrics_from_all_tables("CORR(%s, %s)" % ("pop_day", "jrc_day"), "cor"),
            get_metrics_from_all_tables(
                "CORR(%s, %s)" % ("pop_night", "jrc_night"), "cor"
            ),
            get_metrics_from_all_tables(
                "CORR(Rank_%s, Rank_%s)" % ("pop_day", "jrc_day"), "rank_cor"
            ),
            get_metrics_from_all_tables(
                "CORR(Rank_%s, Rank_%s)" % ("pop_night", "jrc_night"), "rank_cor"
            ),
            get_metrics_from_all_tables("CORR(%s, %s)" % ("dev_day", "jrc_day"), "cor"),
            get_metrics_from_all_tables(
                "CORR(%s, %s)" % ("dev_night", "jrc_night"), "cor"
            ),
            get_metrics_from_all_tables(
                "CORR(Rank_%s, Rank_%s)" % ("dev_day", "jrc_day"), "rank_cor"
            ),
            get_metrics_from_all_tables(
                "CORR(Rank_%s, Rank_%s)" % ("dev_night", "jrc_night"), "rank_cor"
            ),
            get_metrics_from_all_tables(
                "CORR(%s, %s)" % ("dev_act_day", "jrc_day"), "cor"
            ),
            get_metrics_from_all_tables(
                "CORR(%s, %s)" % ("dev_act_night", "jrc_night"), "cor"
            ),
            get_metrics_from_all_tables(
                "CORR(Rank_%s, Rank_%s)" % ("dev_act_day", "jrc_day"), "rank_cor"
            ),
            get_metrics_from_all_tables(
                "CORR(Rank_%s, Rank_%s)" % ("dev_act_night", "jrc_night"), "rank_cor"
            ),
            pd.concat(
                [
                    get_metrics_from_all_tables(
                        "1-0.5*SUM(ABS(%s-%s))/SUM(%s)" % (var, ref, ref),
                        "alloc_accuracy",
                    )
                    for var in var_list
                ]
            ),
            get_metrics_from_all_tables(
                "1-0.5*SUM(ABS(%s-%s))/SUM(%s)" % ("pop_day", "jrc_day", "jrc_day"),
                "alloc_accuracy",
            ),
            get_metrics_from_all_tables(
                "1-0.5*SUM(ABS(%s-%s))/SUM(%s)"
                % ("pop_night", "jrc_night", "jrc_night"),
                "alloc_accuracy",
            ),
        ]
    )
    return metrics


def table3_JOS():
    metrics = get_summary_metrics()
    return (
        metrics.filter(~metrics["value"].isna())
        .groupby("query")
        .agg({"value": "mean", "hours": "count"})
    )


def get_correlation_municipality_levels():

    query = f"""with data as (select 
            jd.population as jrc_day, 
            jn.population as jrc_night, 
            filo_mun.population as filo,
            jd."CODGEO", 
            presence_mun.population_day,
            presence_mun.population_night
            from jrc_mun jd
            inner join jrc_mun jn 
            on jd."CODGEO"=jn."CODGEO"
            inner join filo_mun
            on jd."CODGEO"=filo_mun."CODGEO"
            inner join presence_mun 
            on presence_mun."CODGEO"=jd."CODGEO"
            where jd.time='D'
            and jn.time='N')
            select 
            CORR(jrc_day, population_day),
            CORR(jrc_night, population_night),
            CORR(filo, population_night),
            CORR(filo, population_day),
            CORR(filo, jrc_night),
            CORR(filo, jrc_day)
            from data"""

    l = []
    with engine.connect() as connection:
        res = connection.execute(query).fetchall()
        l += [list(x) for x in res]
    return pd.DataFrame(
        l,
        columns=[
            "corr_jrc_pres_day",
            "corr_jrc_pres_night",
            "corr_filo_pres_night",
            "corr_filo_pres_day",
            "corr_filo_jrc_night",
            "corr_filo_jrc_day",
        ],
    )


if __name__ == "__main__":
    # save_metrics()
    get_metrics_JRC()
