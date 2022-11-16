"""
Postgresql database applications.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.types import Text, Float, ARRAY
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
import pandas as pd
import geopandas as gpd
import datetime as dt
from mobitic_utils.constants import (
    POSTGRESQL_USER,
    POSTGRESQL_PASSWORD,
    POSTGRESQL_HOST,
    POSTGRESQL_PORT,
)
from mobitic_utils.requests_s3 import (
    get_population,
    get_devices,
    get_grille,
    get_filosofi,
    get_JRC,
    get_active_mobiles_FRphones,
    get_area_attraction,
)
from mobitic_utils.transforms import calculate_average_weeks, format_total_population
from mobitic_utils.utils_s3 import write_pandas_s3

engine = create_engine(
    f"postgresql://{POSTGRESQL_USER}:{POSTGRESQL_PASSWORD}\
@{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/defaultdb"
)


def export_dataframe_to_db(
    dataframe: pd.DataFrame, table_name: str, if_exists="replace"
):
    """Export dataframe to the current db given a table name.
    By default, the table will be replaced be replaced by the exported table

    Args:
        dataframe (pd.DataFrame): Dataframe to export
        table_name (str): Sql table name
        if_exists (str): ['fail', 'append', 'replace']
    """

    dataframe.to_sql(
        table_name,
        engine,
        method="multi",
        index=False,
        chunksize=10000,
        if_exists=if_exists,
    )


def export_geopandas_to_db(
    geodataframe: gpd.GeoDataFrame,
    table_name: str,
    srid: int,
    if_exists="replace",
    geometry="Polygon",
):
    """Export a geodataframe with POLYGON geometries to sql.

    Args:
        geodataframe (gpd.GeoDataFrame): Geodataframe to export
        table_name (str): Sql table name
        srid (int): EPSG of the geography
        if_exists (str): ['fail', 'append', 'replace']
        geometry (str): Geometry of the geometry column
    """

    if "MultiPolygon" in geodataframe.geometry.type.to_list():
        geodataframe = _explode(geodataframe)

    geodataframe["geometry"] = geodataframe["geometry"].apply(
        lambda x: WKTElement(x.wkt, srid=srid)
    )

    geodataframe.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
        dtype={"geometry": Geometry(geometry, srid=srid)},
        chunksize=50000,
    )
    return "Done!"


def check_column_exist(table_name: str, column: str):

    query = f"""SELECT EXISTS (
                SELECT 1 from information_schema.columns 
                WHERE table_name='{table_name}'
                AND column_name='{column}'
                );
                """

    with engine.connect() as connection:
        res = connection.execute(query).fetchone()[0]

    return res


def export_average_population_to_db(table_name) -> None:
    """Import population, get the average week value, and export it to the db

    Args:
        table_name (str): Sql table name
    """
    population = get_population(clean=True)
    population = calculate_average_weeks(population)
    export_dataframe_to_db(population, table_name)


def export_total_population_to_db(table_name) -> None:
    """Import population, format the table, and export it to the db

    Args:
        table_name (str): Sql table name
    """
    population = get_population(clean=True)
    population = format_total_population(population)
    export_dataframe_to_db(population, table_name)


def export_devices_to_db(table_name) -> None:
    """Import population, format the table, and export it to the db

    Args:
        table_name (str): Sql table name
    """
    population = get_devices(clean=True)
    population = format_total_population(population)
    export_dataframe_to_db(population, table_name)


def export_active_mobiles_to_db(table_name) -> None:
    """Import active mobiles count, format the table, and export it to the db

    Args:
        table_name (str): Sql table name
    """
    mobiles = get_active_mobiles_FRphones(clean=True)
    mobiles = format_total_population(mobiles)
    export_dataframe_to_db(mobiles, table_name)


def export_derivative_population_to_db(table_name) -> None:
    """export y_t = pop_t - pop_t-1 to the db.

    Args:
        table_name (str): Sql table name
    """
    population = get_population(clean=True)
    population = -population.diff(axis=1, periods=-1)
    population = format_total_population(population)
    export_dataframe_to_db(population, table_name)


def export_intraday_population_to_db(table_name) -> None:
    """export y = pop_t/pop_midnight to the db.

    Args:
        table_name (str): Sql table name
    """
    population = get_population(clean=True)
    population = format_total_population(population)
    columns_to_normalize = list(
        filter(lambda x: x != "x-y" and x != "week", population.columns)
    )
    for i, col in reversed(list(enumerate(columns_to_normalize))):
        population[col] = population[col] / population[columns_to_normalize[i - i % 24]]
    export_dataframe_to_db(population, table_name)


def export_grille(table_name) -> None:
    """export grille.

    Args:
        table_name (str): Sql table name
    """
    grille = get_grille(clean=True)
    export_geopandas_to_db(grille, table_name, srid=27572)


def export_area_attraction(table_name) -> None:
    """export grille.

    Args:
        table_name (str): Sql table name
    """
    area_attraction = get_area_attraction()
    export_geopandas_to_db(area_attraction, table_name, srid=27572)


def export_filosofi(table_name) -> None:
    """export filosofi.

    Args:
        table_name (str): Sql table name
    """
    filosofi = get_filosofi()
    export_geopandas_to_db(
        filosofi, "original_" + table_name, srid=27572, geometry="Polygon"
    )

    query = f"""CREATE TABLE {table_name} AS
                SELECT SUM(nbpersm)/ST_AREA(g.geometry)*1000000 as population, 
                g.geometry as geometry,
                g."x-y" as "x-y"
                FROM original_{table_name} f, grille g
                WHERE ST_contains(g.geometry, f.geometry)
                GROUP BY g.geometry, g."x-y"
                """
    with engine.connect() as connection:
        connection.execute(query)


def export_JRC(table_name) -> None:
    """export JRC.

    Args:
        table_name (str): Sql table name
    """
    jrc = get_JRC()
    export_geopandas_to_db(
        jrc, "original_" + table_name, srid=27572, geometry="Polygon"
    )

    query = f"""CREATE TABLE {table_name} AS
                SELECT 
                    SUM(o.population * ST_Area(ST_INTERSECTION(o.geometry, g.geometry))) / ST_AREA(g.geometry) as population,
                    g.geometry as geometry,
                    o.time as day_night,
                    o.month as month,
                    g."x-y" as "x-y"
                    FROM original_{table_name} o, grille g
                    WHERE ST_INTERSECTS(o.geometry, g.geometry)
                    GROUP BY g.geometry, g."x-y", o.time, o.month
                """

    with engine.connect() as connection:
        connection.execute(query)


def create_municipality(
    table_name,
    type="jrc",
    month="3",
    time_day="1-3",
    time_night="1-15",
    week="12",
    table_presence="total_population",
) -> None:
    """Create municipality-level population files.
    Args:
        table_name (str): Sql table name
    """
    if type == "jrc":
        query_create = f"""CREATE TABLE {table_name} AS
        SELECT SUM(o.population * ST_Area(ST_INTERSECTION(o.geometry, aa.geometry))/ST_Area(o.geometry)) as population,
                        aa.geometry as geometry,
                        aa."CODGEO",
                        o.time
                        FROM original_jrc o, area_attraction aa
                        WHERE ST_INTERSECTS(o.geometry, aa.geometry) AND o.month='{month}'
                        GROUP BY aa.geometry, aa."CODGEO", o.time """

    if type == "filosofi":
        query_create = f"""CREATE TABLE {table_name} AS
                        SELECT SUM(o.nbpersm* ST_Area(ST_INTERSECTION(o.geometry, aa.geometry))/ST_AREA(o.geometry)) as population,
                        aa.geometry as geometry,
                        aa."CODGEO"
                        FROM original_filosofi o, area_attraction aa
                        WHERE ST_INTERSECTS(o.geometry, aa.geometry)
                        GROUP BY aa.geometry, aa."CODGEO"
                        """

    if type == "present":
        query_create = f"""CREATE TABLE {table_name} AS
                        SELECT 
                        SUM(pop."{time_night}" * ST_Area(ST_INTERSECTION(g.geometry, aa.geometry)))/1000000 as population_night, 
                        SUM(pop."{time_day}" * ST_Area(ST_INTERSECTION(g.geometry, aa.geometry)))/1000000 as population_day,
                        aa.geometry as geometry,
                        aa."CODGEO"
                        FROM {table_presence} pop, grille g, area_attraction aa
                        where week = '{week}'
                        and g."x-y"=pop."x-y"
                        and ST_INTERSECTS(g.geometry, aa.geometry)
                        GROUP BY aa.geometry, aa."CODGEO"
                        """

    with engine.connect() as connection:
        connection.execute(query_create)


def join_filosofi(table_name, filosofi_table):

    query_add_column = f"""ALTER TABLE {table_name}\
                ADD COLUMN filosofi double precision;
                """

    query_join_data = f"""UPDATE  {table_name}\
                    SET filosofi = f.population\
                    FROM {filosofi_table} f\
                    WHERE {table_name}."x-y" = f."x-y";
                """

    with engine.connect() as connection:
        connection.execute(query_add_column)
        connection.execute(query_join_data)

    return "Done!"


def _explode(indf):
    count_mp = 0
    outdf = gpd.GeoDataFrame(columns=indf.columns)
    outdf = indf[indf.geometry.type == "Polygon"]
    indf = indf[indf.geometry.type != "Polygon"]
    for idx, row in indf.iterrows():
        if type(row.geometry) == MultiPolygon:
            count_mp = count_mp + 1
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row] * recs, ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom, "geometry"] = row.geometry[geom]
            outdf = outdf.append(multdf, ignore_index=True)
        else:
            print(row)
    print("There were ", count_mp, "Multipolygons found and exploded")
    return outdf


def import_gpd_from_sql(sql: str) -> gpd.GeoDataFrame:
    df = gpd.read_postgis(sql, engine, geom_col="geometry")
    return df
