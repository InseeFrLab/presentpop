"""
Request data from s3 application
"""


import io
import boto3
import pandas as pd
import geopandas as gpd
import numpy as np
import re
from shapely.geometry import Polygon, Point
from mobitic_utils.utils_s3 import get_pandas_s3, get_geopandas_s3, get_shapefile
from mobitic_utils.constants import (
    ENDPOINT_URL,
    BUCKET,
    KEY_POPULATION,
    KEY_MOBILES,
    KEY_GRILLE_50,
    KEY_POPULATION_CELL,
    KEY_SM_FV,
    KEY_SM_FV_grid,
    KEY_SM_noFV_grid,
    KEY_FILOSOFI,
    KEY_JRC,
    KEY_IMSI_COUNT_CELL,
    KEY_ACTIVE_MOBILES_OLD,
    KEY_AREA_ATTRACTION,
    KEY_HOME_CELLS,
    KEY_AGG_INTERPOLATION_ERROR,
    KEY_WEIGHTING_RAW,
    KEY_HEALTHCARE,
    KEY_IMSI_COUNT_CELL_INT_SCOPE_PARQUET,
    KEY_IMSI_COUNT_CELL_SCOPE_PARQUET,
)

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)

# CONSTANTS
PROBABILITIES_COLUMNS = ["x_tile", "y_tile", "dma_name", "Pij"]


def get_population_cell_day(Date) -> pd.DataFrame:
    """
    Get population at cell level given the folder of an S3 Object storing all dates and returns one day as a pd.Dataframe.
    The table contains estimation for every hours, for each cells on this date.
    This is the output of the presence estimation (aggregation at cell level) module.

    Args:
        key (str): The S3 folder key
        Date (str): A date formated as 'YYYY/mm/dd'

    Returns:
        pd.DataFrame: Estimated population for every hour and each cell
    """

    Files = [
        f["Key"]
        for f in s3.list_objects(Bucket=BUCKET, Prefix=KEY_POPULATION_CELL)["Contents"]
    ]

    file = [x for x in Files if re.search(Date, x)]

    if len(file) == 1:
        obj = s3.get_object(Bucket=BUCKET, Key=file[0])
        pop = pd.read_parquet(
            io.BytesIO(obj["Body"].read()),
            columns=[
                "ref_hour",
                "lac",
                "ci_sac",
                "res_count",
                "res_count_fixed_w",
                "imsi_count",
            ],
        )
        pop["dma_name"] = pop.lac.astype(str) + "_" + pop.ci_sac.astype(str) + "_1700"
        pop = pop.drop(["lac", "ci_sac"], axis=1)
    else:
        pop = None

    return pop


def get_equivalent_FV(key=KEY_SM_FV) -> pd.DataFrame:
    """
    Get the mapping between signaling cells and Fluxvision cells (FV cells), when existing.

    Fluxvision cells are the cells with a coverage modelisation.
    Signaling cells are the cells with appears in the cell registry (coordinates, technology) and in the raw signalling data.

    Args:
        key (str): The S3 folder key for the mapping

    Returns:
        pd.DataFrame: For each signaling cell, its coordinates (epsg: 27572),
        techno, dma_name (ident) and dma_name_in_fv (equivalent FV cell ident)
    """

    obj = s3.get_object(Bucket=BUCKET, Key=key)
    mapping = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        usecols=["dma_name", "COORD_X", "COORD_Y", "TECHNO", "dma_name_in_fv"],
    )
    return mapping


def get_grid_proj_FV(key=KEY_SM_FV_grid) -> pd.DataFrame:
    """
    Get the spatial mapping from Fluxvision cells to the dissemination grid.
    Corresponds to matrix Q in the JOS paper.

    Args:
        key (str): The S3 folder key for the mapping

    Returns:
        pd.DataFrame: For each FV cell, get the projection in x_tile,
        y_tile via the conditional probability to be in the tile knowing the cell (dma_name) - with a
    """

    obj = s3.get_object(Bucket=BUCKET, Key=key)
    mapping = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        usecols=["dma_name", "x_tile_up", "y_tile_up", "p_car_cond_ant_prior_unif"],
    )
    mapping = mapping.rename(columns={"p_car_cond_ant_prior_unif": "Pij"})

    return mapping


def get_grid_proj_noFV(key=KEY_SM_noFV_grid) -> pd.DataFrame:
    """Get the spatial mapping from cells without FV equivalent to the grid
    Args:
        key (str): The S3 folder key for the mapping

    Returns:
        pd.DataFrame: For each cell in FV, get the projection in x_tile,
        y_tile via the conditional probability to be in the tile knowing the cell (dma_name) - with a
    """

    obj = s3.get_object(Bucket=BUCKET, Key=key)
    mapping = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        usecols=["dma_name", "x_tile_up", "y_tile_up", "Pij"],
    )

    return mapping


def get_population(key=KEY_POPULATION, clean=False) -> pd.DataFrame:
    """Get latest estimated population given a key of an S3 Object and return it as a pd.Dataframe.
    The table contains estimation for every hours, day, for every x_tile_up, x_tile_up.

    Args:
        key (str): The S3 key
        clean (str): A cleaning process to regroup x and y tiles,
            transform dates in to datetime types, and set the index to x-y

    Returns:
        pd.DataFrame: Estimated population for every hour date
    """

    populations = get_pandas_s3(Key=key)
    populations = populations.reset_index()
    if clean:
        populations["x-y"] = (
            populations["x_tile_up"].astype(str)
            + "-"
            + populations["y_tile_up"].astype(str)
        )
        populations = populations.drop(["x_tile_up", "y_tile_up"], axis=1)
        populations = populations.set_index("x-y")
        population_columns = populations.columns + " 2019"
        dates = pd.to_datetime(population_columns, format="%m-%d %H %Y")
        populations.columns = dates

    return populations


def get_devices(key=KEY_MOBILES, clean=False) -> pd.DataFrame:
    """Get latest estimated present devices (with trajectory interpolation, without residency reweighting) given a key of an S3 Object and return it as a pd.Dataframe.
    The table contains estimation for every hours, day, for every x_tile_up, x_tile_up.


    Args:
        key (str): The S3 key
        clean (str): A cleaning process to regroup x and y tiles,
            transform dates in to datetime types, and set the index to x-y

    Returns:
        pd.DataFrame: Estimated devices for every hour date
    """

    populations = get_pandas_s3(Key=key)
    populations = populations.reset_index()
    if clean:
        populations["x-y"] = (
            populations["x_tile_up"].astype(str)
            + "-"
            + populations["y_tile_up"].astype(str)
        )
        populations = populations.drop(["x_tile_up", "y_tile_up"], axis=1)
        populations = populations.set_index("x-y")
        population_columns = populations.columns + " 2019"
        dates = pd.to_datetime(population_columns, format="%m-%d %H %Y")
        populations.columns = dates

    return populations


def get_grille(key=KEY_GRILLE_50, clean=True) -> gpd.GeoDataFrame:
    """Get latest grille 50 outputed by the quadtree algorithm

    Args:
        key (str): The S3 key of the grille file
        clean (str): A cleaning process to regroup x and y tiles,
    and remove not essential information: 'x_tile_up', 'y_tile_up', 'scale', 'test', 'n_down', 'id'

    Returns:
        gpd.GeoDataFrame: Grille giving geometry information of
            every x_tile_up, y_tile_up, in epsg 27572
    """

    grille = get_geopandas_s3(Key=key, epsg=27572)

    if clean:
        grille["x-y"] = (
            grille["x_tile_up"].astype(str) + "-" + grille["y_tile_up"].astype(str)
        )
        grille = grille.drop(
            ["x_tile_up", "y_tile_up", "scale", "test", "n_down", "id"], axis=1
        )

    return grille


def get_area_attraction(key=KEY_AREA_ATTRACTION, reduce=False) -> gpd.GeoDataFrame:
    """Get area attraction

    Args:
        key (str): The S3 key of the grille file

    Returns:
        gpd.GeoDataFrame: rich area information
    """

    df = get_shapefile(Key=key)
    df = df.to_crs("EPSG:27572")
    return df


def get_filosofi(key=KEY_FILOSOFI, drop_tileID=True) -> gpd.GeoDataFrame:
    """Get filosofi data corresponding to people density data aggregated at the
    FV grille (100m, crs=27572), with men = #Menage, and nbpersm = #Personne

    Args:
        key (str): The S3 key of the filosofi file

    Returns:
        gpd.GeoDataFrame: Filosofi density data aggregated at the
    FV grille (100m, crs=27572), with men = #Menage, and nbpersm = #Personne
    """

    filosofi = get_pandas_s3(Key=key)

    geometry = [
        Polygon([(x, y), (x + 100, y), (x + 100, y + 100), (x, y + 100)])
        for x, y in zip(filosofi["x_tile"], filosofi["y_tile"])
    ]
    if drop_tileID:
        filosofi = filosofi.drop(["x_tile", "y_tile"], axis=1)
    filosofi = gpd.GeoDataFrame(filosofi, crs="EPSG:27572", geometry=geometry)

    return filosofi


def get_JRC(key=KEY_JRC) -> gpd.GeoDataFrame:

    l = []
    for day_night in ["N", "D"]:
        for month in range(3, 6):
            l += [get_pandas_s3(Key=key % (day_night, month))]

    df = pd.concat(l)

    geometry = [
        Polygon([(x, y), (x + 1000, y), (x + 1000, y + 1000), (x, y + 1000)])
        for x, y in zip(df["x"], df["y"])
    ]
    df = df.rename(columns={"value": "population"})
    df = df.drop(["x", "y"], axis=1)
    df = gpd.GeoDataFrame(df, crs="EPSG:3035", geometry=geometry)
    df = df.to_crs("EPSG:27572")
    df = df[df.geometry.is_valid]

    return df


def get_healthcare_infrastructure(key=KEY_HEALTHCARE) -> gpd.GeoDataFrame:
    df = get_geopandas_s3(KEY_HEALTHCARE, 4326)
    df = df[df.columns[df.count() == 616]]
    df = df.to_crs("EPSG:27572")
    df.geometry = df.geometry.centroid
    return df


def get_active_imsi_count_cell(key=KEY_IMSI_COUNT_CELL, on_s3=True) -> pd.DataFrame:
    """Get hourly imsi counts of active devices, compared with interpolated imsi counts, at cell level

    Args:
        key (str): The S3 key of the file folder
        on_s3 (bool): If true, get the file from s3 - otherwise compute it from parquet files

    Returns:
        pd.DataFrame: Active (imsi_count) or Interpolated (imsi_count_int) devices per hour and cells
    """
    if on_s3 == True:
        pop = get_pandas_s3(KEY_IMSI_COUNT_CELL)
    else:
        pop = get_active_imsi_count_cell_by_date("16")
        for Date in range(17, 32):
            print(Date)
            pop = pd.concat([pop, get_active_imsi_count_cell_by_date(str(Date))])
    return pop


def get_agg_interpolation_error_by_date(Date):
    """Get error in hours from interpolation, hourly imsi counts of active devices, compared with interpolated imsi counts, by technology

    Args:
        Date (str): Selecting the Date between 16 to 31 (march)

    Returns:
        pd.DataFrame: Sum of differences ref_hour, observed ours for all imsi in scope, by ref_hour and techno
    """

    cells = get_equivalent_FV()[["dma_name", "TECHNO"]]

    pop = get_active_imsi_count_cell_by_date(Date)
    pop = pd.merge(pop, cells, on="dma_name")
    pop["sum_diff_hour"] = pop.mean_diff_hour * pop.imsi_count_int
    error_techno = (
        pop.groupby(["TECHNO", "ref_hour"])
        .agg({"imsi_count_int": "sum", "imsi_count": "sum", "sum_diff_hour": "sum"})
        .reset_index()
    )
    pop["TECHNO"] = "All"
    error = (
        pop.groupby(["TECHNO", "ref_hour"])
        .agg({"imsi_count_int": "sum", "imsi_count": "sum", "sum_diff_hour": "sum"})
        .reset_index()
    )
    errors = pd.concat([error, error_techno])

    Files = [
        f["Key"]
        for f in s3.list_objects(Bucket=BUCKET, Prefix=KEY_POPULATION_CELL)["Contents"]
    ]

    file = [x for x in Files if re.search("2019/03/" + Date, x)]

    if len(file) == 1:
        obj = s3.get_object(Bucket=BUCKET, Key=file[0])
        pop = pd.read_parquet(
            io.BytesIO(obj["Body"].read()),
            columns=[
                "ref_hour",
                "lac",
                "ci_sac",
                "res_count_fixed_w",
                "sum_diff_hour_res",
            ],
        )
        pop["dma_name"] = pop.lac.astype(str) + "_" + pop.ci_sac.astype(str) + "_1700"
        pop = pop.drop(["lac", "ci_sac"], axis=1)
        pop = pd.merge(pop, cells, on="dma_name")
        error_techno = (
            pop.groupby(["TECHNO", "ref_hour"])
            .agg({"res_count_fixed_w": "sum", "sum_diff_hour_res": "sum"})
            .reset_index()
        )
        pop["TECHNO"] = "All"
        error = (
            pop.groupby(["TECHNO", "ref_hour"])
            .agg({"res_count_fixed_w": "sum", "sum_diff_hour_res": "sum"})
            .reset_index()
        )
        error = pd.concat([error, error_techno])

    return errors, error


def get_agg_interpolation_error():

    error, error2 = get_agg_interpolation_error_by_date("17")

    for Date in range(18, 18 + 7):
        err, err2 = get_agg_interpolation_error_by_date(str(Date))
        error = pd.concat([error, err])
        error2 = pd.concat([error2, err2])

    error = pd.merge(error, error2, on=["TECHNO", "ref_hour"])

    return error


def get_active_imsi_count_cell_by_date(Date="18") -> pd.DataFrame:
    """Get hourly imsi counts of active devices, compared with interpolated imsi counts, at cell level

    Args:
        Date (str): Selecting the Date between 16 to 31 (march)

    Returns:
        pd.DataFrame: Active (imsi_count) or Interpolated (imsi_count_int) devices per hour and cells
    """
    file_int = [
        f["Key"]
        for f in s3.list_objects(
            Bucket=BUCKET, Prefix=KEY_IMSI_COUNT_CELL_INT_SCOPE_PARQUET
        )["Contents"]
        if re.search(".parquet$", f["Key"]) and re.search("/03/" + Date, f["Key"])
    ]

    file_noint = [
        f["Key"]
        for f in s3.list_objects(
            Bucket=BUCKET, Prefix=KEY_IMSI_COUNT_CELL_SCOPE_PARQUET
        )["Contents"]
        if re.search(".parquet$", f["Key"]) and re.search("/03/" + Date, f["Key"])
    ]

    if len(file_int) == 1 & len(file_noint) == 1:
        obj = s3.get_object(Bucket=BUCKET, Key=file_int[0])
        pop = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        pop = pop[
            (pop.is_phone == True)
            & (pop.orange == True)
            & (pop.roamer_foreign == False)
        ]
        pop = pop[["ref_hour", "lac", "ci_sac", "imsi_count", "mean_diff_hour"]]
        pop = pop.rename({"imsi_count": "imsi_count_int"}, axis=1)

        obj2 = s3.get_object(Bucket=BUCKET, Key=file_noint[0])
        pop2 = pd.read_parquet(io.BytesIO(obj2["Body"].read()))

        pop2 = pop2[
            (pop2.is_phone == True)
            & (pop2.orange == True)
            & (pop2.roamer_foreign == False)
        ]
        pop2 = pop2[["hour", "lac", "ci_sac", "imsi_count"]]
        pop2 = pop2.rename({"hour": "ref_hour"}, axis=1)

        pop = pd.merge(pop, pop2, on=["ref_hour", "lac", "ci_sac"])

        pop["dma_name"] = pop.lac.astype(str) + "_" + pop.ci_sac.astype(str) + "_1700"
        pop = pop.drop(["lac", "ci_sac"], axis=1)
    else:
        pop = None

    return pop


def get_interpolation_error(
    key=KEY_IMSI_COUNT_CELL,
    Dates=["03-18", "03-19", "03-20", "03-21", "03-22", "03-23", "03-24"],
) -> pd.DataFrame:
    """Get hourly imsi counts of active devices, compared with interpolated imsi counts, at cell level

    Args:
        key (str): The S3 key of the file folder
        Dates (list str): Selecting the Dates between 16-03 to 31-03

    Returns:
        pd.DataFrame: Active (imsi_count) or Interpolated (imsi_count_int) devices per hour and cells
    """

    file = [
        f["Key"]
        for f in s3.list_objects(Bucket=BUCKET, Prefix=key)["Contents"]
        if re.search(".csv$", f["Key"])
    ]

    if len(file) == 1:
        obj = s3.get_object(Bucket=BUCKET, Key=file[0])
        pop = pd.read_csv(
            io.BytesIO(obj["Body"].read()),
            usecols=[0, 1, 2, 6, 7, 8],
            names=[
                "ref_hour",
                "lac",
                "ci_sac",
                "imsi_count_int",
                "imsi_count",
                "mean_diff_hour",
            ],
            iterator=True,
            chunksize=10000,
        )
        pop = pd.concat(
            [chunk[chunk.ref_hour.apply(lambda x: x[0:5]).isin(Dates)] for chunk in pop]
        )
        pop["dma_name"] = pop.lac.astype(str) + "_" + pop.ci_sac.astype(str) + "_1700"
        pop = pop.drop(["lac", "ci_sac"], axis=1)

    else:
        pop = None

    return pop


def get_active_mobiles_FRphones(
    key=KEY_ACTIVE_MOBILES_OLD, clean=False
) -> pd.DataFrame:
    """Get active devices counts at tile level given a key of an S3 Object and return it as a pd.Dataframe.
    The table contains estimation for every hours, day, for every x_tile_up, x_tile_up over one week of march.

    Args:
        key (str): The S3 key
        clean (str): A cleaning process to regroup x and y tiles,
            transform dates in to datetime types, and set the index to x-y

    Returns:
        pd.DataFrame: Active devices counts for every hour date
    """

    mobiles = get_pandas_s3(Key=key)
    mobiles = mobiles.reset_index()
    if clean:
        mobiles["x-y"] = (
            mobiles["x_tile_up"].astype(str) + "-" + mobiles["y_tile_up"].astype(str)
        )
        mobiles = mobiles.drop(["x_tile_up", "y_tile_up"], axis=1)
        mobiles = mobiles.set_index("x-y")
        population_columns = mobiles.columns + " 2019"
        dates = pd.to_datetime(population_columns, format="%m-%d %H %Y")
        mobiles.columns = dates

    return mobiles


def get_home_cells(scheme="1") -> pd.DataFrame:
    """Get the home cells file for each weighting scheme

    Args:
        scheme (str): '1', '2' or '34' which stand for all time, nighttime, distinct days home detection
    Returns:
        pd.DataFrame: Detected mobile residents (column count) per home cell (column dma_name)
    """

    File = [
        f["Key"]
        for f in s3.list_objects(Bucket=BUCKET, Prefix=KEY_HOME_CELLS % scheme)[
            "Contents"
        ]
        if re.search(".csv$", f["Key"])
    ]
    if len(File) == 1:
        obj = s3.get_object(Bucket=BUCKET, Key=File[0])
        var_names = {
            "1": ["in_scope", "lac", "ci_sac", "count"],
            "2": ["lac", "ci_sac", "count"],
            "34": ["in_scope", "in_time_range", "lac", "ci_sac", "count"],
        }
        homecells = pd.read_csv(io.BytesIO(obj["Body"].read()), names=var_names[scheme])

        if scheme == "1":
            homecells = homecells[homecells.in_scope == True]

        if scheme == "34":
            homecells = homecells[
                (homecells.in_scope == True) & (homecells.in_time_range == False)
            ]

        homecells["dma_name"] = (
            homecells.lac.astype(str) + "_" + homecells.ci_sac.astype(str) + "_1700"
        )
        homecells = homecells[["dma_name", "count"]]

    else:
        homecells = None

    return homecells


def get_residents_on_home_cells(scheme="1") -> pd.DataFrame:
    """Get the filosofi residents in each home cells for each weighting scheme.

    About 30 000 persons (over 62.74 millions) are not recovered in home cells after spatial mapping (possibly linked to cells which are not in homecells)

    Args:
        scheme (str): '1', '2' or '34' which stand for all time, nighttime, distinct days home detection
    Returns:
        pd.DataFrame: Filosofi residents per home cell
    """

    homecells = pd.concat(
        [
            get_pandas_s3(Key=KEY_WEIGHTING_RAW % scheme + ".csv"),
            get_pandas_s3(Key=KEY_WEIGHTING_RAW % scheme + "_marginal.csv"),
        ]
    )
    homecells = homecells.groupby(["dma_name"]).agg({"n_res": "sum"}).reset_index()

    return homecells
