import boto3
import io
import os
import geopandas as gpd
import pandas as pd
import numpy as np
import re

from mobitic_utils.requests_s3 import (
    get_grille,  # Dissemination grid
    get_population_cell_day,  # Presence estimation at cell level
    get_active_imsi_count_cell,  # Active devices count at cell level
    get_equivalent_FV,  # Mapping between the data cells and the cells from matrix A (origin: Fluxvision, FV)
    get_grid_proj_FV,  # Spatial Mapping Q for FV cells (with coverage modelisation)
    get_grid_proj_noFV,  # Spatial Mapping Q for cells without coverage modelisation
)

# Paths on S3
from mobitic_utils.constants import (
    ENDPOINT_URL,  # S3 endpoint
    BUCKET,  # S3 bucket
    KEY_SM_FV,  # S3 Object Key for get_equivalent_FV data
    KEY_SM_FV_grid,  # S3 Object Key for get_grid_proj_FV data
    KEY_POPULATION_CELL,  # S3 Object Key for get_population_cell_day
    KEY_SM_noFV_grid,  # S3 Object Key for get_grid_proj_noFV data
    KEY_IMSI_COUNT_CELL,  # S3 Object Key for get_active_imsi_count_cell data
    KEY_FV_BARYCENTER,  # S3 Objet Key for FV cells coverage barycenter
)
from mobitic_utils.utils_s3 import write_pandas_s3, get_pandas_s3


def spatial_map(popcells, mapping_tiles, imsi_only=False) -> pd.DataFrame:
    """
    Compute population at tile level, given population estimates at cell level and a spatial mapping.
    Corresponds to module "spatial mapping" in the Journal of Official Statistics paper.

    Args:
        popcells : (pd.DataFrame) Population (imsi, res) at cell(dma_name) x ref_hour level
        mapping_tiles : (pd.DataFrame) Qij - spatial mapping
        imsi_only: (boolean) Retrieve only imsi count

    Returns:
        pd.DataFrame: Present population per tile and reference hour
    """
    popTiles = pd.merge(
        popcells, mapping_tiles, left_on="dma_name_map", right_on="dma_name"
    )

    if imsi_only == False:
        popTiles = (
            popTiles.assign(
                imsi=popTiles.imsi_count * popTiles.Pij,
                res=popTiles.res_count_fixed_w * popTiles.Pij,
                res_present=popTiles.res_count * popTiles.Pij,
            )
            .groupby(["x_tile_up", "y_tile_up", "ref_hour"], as_index=False)[
                ["imsi", "res"]
            ]
            .agg(func=sum)
        )
    else:
        popTiles = (
            popTiles.assign(imsi=popTiles.imsi_count * popTiles.Pij)
            .groupby(["x_tile_up", "y_tile_up", "ref_hour"], as_index=False)[["imsi"]]
            .agg(func=sum)
        )
    return popTiles


def define_mapping_cells() -> pd.DataFrame:
    """For each cell in signaling aggregates, provide an equivalent cell to
    use in the spatial mapping.
    The equivalent cell is
    1. the Fluxvision cell when it exists
    2. the Fluxvision cell at the same x,y
    location with the same techno (2G,3G,4G) which
    covers the largest #of tiles
    3. itself when not in previous case.
    Returns:
        pd.DataFrame: For each cell in the device presence
        panel data, its equivalent cell.
    """
    mapping_cells = get_equivalent_FV(KEY_SM_FV)
    mapping_cells["dma_name_map"] = mapping_cells["dma_name_in_fv"].fillna(
        mapping_cells["dma_name"]
    )
    return mapping_cells[["dma_name", "dma_name_map"]]


def define_mapping_tiles() -> pd.DataFrame:
    """For each equivalent cell of cells in device presence panel,
    provide the spatial mapping in the reduced grid.
        The equivalent cell is
    1. the Fluxvision cell when it exists
    2. the Fluxvision cell at the same x,y
    location with the same techno (2G,3G,4G) which
    covers the largest #of tiles
    3. itself when not in previous case.

    Spatial mapping of equivalent cells:
    1 and 2. with the equivalent FV coverage map
    (uniform prior to aggregate)
    3. mapped uniformly over tiles intersecting
    a 200m buffer around coordinates.

    Returns:
        pd.DataFrame: For each equivalent cells, spatial mapping
        over tiles in the grid (Qij)
    """
    mapping_tiles_1 = get_grid_proj_FV(KEY_SM_FV_grid)
    mapping_tiles_2 = get_grid_proj_noFV(KEY_SM_noFV_grid)
    mapping_tiles = pd.concat([mapping_tiles_1, mapping_tiles_2])
    return mapping_tiles


def all_dates(Key_read: str):
    """From date-specific files, build one all-date file"""

    s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)

    Files = [
        f["Key"] for f in s3.list_objects(Bucket=BUCKET, Prefix=Key_read)["Contents"]
    ]

    dfs = [get_pandas_s3(File) for File in Files]

    df = pd.concat(dfs)

    return df


def get_cells_point_geography():
    """
    Get cell POINT geography information:
    - tower cell coordinates
    - the barycenter of the coverage FV cell (for its equivalent cell)
    under a uniform prior
    Returns:
        pd.DataFrame: Cells point Geo (COORD_X, COORD_Y): tower coordinates
                                      dma_name_map: equivalent cell
                                      (barycentre_antenne_x_unif,barycentre_antenne_y_unif) : barycenter
    """
    cells = define_mapping_cells()
    coords = get_equivalent_FV(KEY_SM_FV)[["dma_name", "COORD_X", "COORD_Y"]]
    baryctr = get_pandas_s3(KEY_FV_BARYCENTER)
    baryctr = baryctr[
        ["dma_name", "barycentre_antenne_x_unif", "barycentre_antenne_y_unif"]
    ].rename({"dma_name": "dma_name_map"}, axis=1)

    cells = pd.merge(cells, coords, on="dma_name")
    cells = pd.merge(cells, baryctr, on="dma_name_map", how="left")

    return cells


if __name__ == "__main__":

    s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)

    mapping_task = "resident"

    mapping_cells = define_mapping_cells()

    mapping_tiles = define_mapping_tiles()

    if mapping_task == "resident":

        KEY_WRITE = "CallMe/cancan/hourly_tiles_grid50/"

        Files = [
            f["Key"]
            for f in s3.list_objects(Bucket=BUCKET, Prefix=KEY_POPULATION_CELL)[
                "Contents"
            ]
        ]
        dates = [re.sub(KEY_POPULATION_CELL, "", x)[0:10] for x in Files]

        if "Contents" in s3.list_objects(Bucket=BUCKET, Prefix=KEY_WRITE):
            Files = [
                f["Key"]
                for f in s3.list_objects(Bucket=BUCKET, Prefix=KEY_WRITE)["Contents"]
            ]

            dates_done = [re.sub(KEY_WRITE, "", x)[0:10] for x in Files]

            dates = [date for date in dates if date not in dates_done]

        for Date in dates:

            print(Date)

            pop = get_population_cell_day(Date)

            pop = pd.merge(pop, mapping_cells, on="dma_name")

            popchunks = np.array_split(pop, 20)

            list_pop = [spatial_map(popsub, mapping_tiles) for popsub in popchunks]

            popTiles = pd.concat(list_pop)

            popTiles = popTiles.groupby(["x_tile_up", "y_tile_up", "ref_hour"])[
                ["imsi", "res"]
            ].agg(func=sum)

            KEY_WRITE_DATE = KEY_WRITE + Date + "/estimates.csv"

            write_pandas_s3(popTiles, KEY_WRITE_DATE)

    if mapping_task == "active_imsi":

        KEY_WRITE = "CallMe/cancan/hourly_tiles_grid50_active/"

        pop0 = get_active_imsi_count_cell(key=KEY_IMSI_COUNT_CELL)
        pop0 = pop0[["ref_hour", "dma_name", "imsi_count"]]
        pop0.rename({"imsi_count": "imsi"}, axis=1)

        for Date in ["03-18", "03-19", "03-20", "03-21", "03-22", "03-23", "03-24"]:

            print(Date)
            pop = pop0[pop0.ref_hour.apply(lambda x: x[0:5]) == Date]
            pop = pd.merge(pop, mapping_cells, on="dma_name")

            popchunks = np.array_split(pop, 20)

            list_pop = [
                spatial_map(popsub, mapping_tiles, imsi_only=True)
                for popsub in popchunks
            ]

            popTiles = pd.concat(list_pop)

            popTiles = popTiles.groupby(["x_tile_up", "y_tile_up", "ref_hour"])[
                ["imsi"]
            ].agg(func=sum)

            KEY_WRITE_DATE = KEY_WRITE + Date + "/estimates.csv"

            write_pandas_s3(popTiles, KEY_WRITE_DATE)
