import pandas as pd
import boto3
import io
from sklearn.neighbors import KDTree
import networkx as nx


from mobitic_utils.constants import (
    ENDPOINT_URL,
    BUCKET,
    KEY_HOME_CELLS,
    KEY_FILOSOFI,
    KEY_FV_100,
    KEY_WEIGHTING_RAW,
    KEY_REAFFECT_TILES,
    KEY_WEIGHTING_GROUP,
    KEY_WEIGHTING_EVAL,
    KEY_WEIGHTING,
)

from mobitic_utils.spatial_mapping import (
    define_mapping_cells,
    get_cells_point_geography,
)
from mobitic_utils.requests import (
    get_home_cells,
    get_filosofi,
    get_pandas_s3,
    get_residents_on_home_cells,
)
from mobitic_utils.utils_s3 import write_pandas_s3

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)


def home_cells_equivalent_cells(scheme="all"):
    """
    Get for each potential home cells (all three weighting schemes or
    a particular) a candidate FV cell (with coverage modelisation)

    Args:
        scheme (str): '1', '2' or '34' which stand for all time, nighttime,
        distinct days home detection
    Returns:
        pd.DataFrame: A candidate FV cells per home cell
    """
    if scheme == "all":
        homecells = pd.concat(
            [get_home_cells("1"), get_home_cells("2"), get_home_cells("34")]
        )
    else:
        homecells = get_home_cells(scheme)

    homecells = homecells[["dma_name"]].drop_duplicates()
    linkedcells = define_mapping_cells()

    linkedcells = pd.merge(homecells, linkedcells, on="dma_name", how="left")
    return linkedcells


def get_FV_files(key=KEY_FV_100):
    """Get Files names for FV maps cut in 100km2
    Returns:
        list str: S3 Keys
    """
    all_objects = s3.list_objects(Bucket=BUCKET, Prefix=key)
    files = [obj["Key"] for obj in all_objects["Contents"][1:]]
    files = [
        s3.list_objects(Bucket=BUCKET, Prefix=file, Delimiter="/")["Contents"][0]["Key"]
        for file in files
    ]
    return files


def get_FV_master100(key):
    """Get coverage map over a tiles of 100 km2 given its S3 Key
    Args:
        key (str): S3 key
    Returns:
        pd.DataFrame: FV coverage map restricted to tiles in the master tiles of 100 km2
    """

    obj = s3.get_object(Bucket=BUCKET, Key=key)

    fv = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        usecols=[0, 1, 2, 6],
        names=["x_tile", "y_tile", "dma_name", "proba1"],
    )

    return fv


def projection_residency_to_homecells_master_tile(key_fv, filo, scheme="1"):
    """
    Project Filosofi residents on home cells by:
    1. Getting P(connect to j | tile i) for tile i a tile with Filosofi
    residents and j a FV cell (a cell with coverage from Fluxvision)
    2. Getting home cells subset j' and their equivalent FV cells j
    3. Distributing residents to linked home cells with
    P(connect to j' | tile i) proportionally to P(j | i)

    This step is done by mega tiles of 100km.

    Args:
        key_fv (str): S3 key for FV coverage map (preferably restricted to
        tiles in the master tiles of 100 km2)
        filo (pd.DataFrame): Filosofi residents on the 100m grid
        scheme (str): '1', '2' or '34' which stand for all time, nighttime,
        distinct days home detection
    Returns:
        pd.DataFrame: Resident counts on home cells
    """
    print(key_fv)

    # Tiles with residents and their cell coverage in the 100km master file
    filo_in_fv = pd.merge(get_FV_master100(key_fv), filo, on=["x_tile", "y_tile"])
    filo_in_fv = filo_in_fv.rename({"dma_name": "dma_name_map"}, axis=1)

    if filo_in_fv.shape[0] != 0:

        # Restriction to home cells
        homecells = home_cells_equivalent_cells(scheme)
        homecells = pd.merge(homecells, filo_in_fv, on="dma_name_map")

        # Connection to cells which are not homecells
        # are redistributed to homecells
        temp = homecells.groupby(["x_tile", "y_tile"], group_keys=False).apply(
            lambda p: p.proba1 / p.proba1.sum()
        )

        if temp.shape[0] == homecells.shape[0]:
            homecells["proba"] = temp
        elif temp.shape[1] == homecells.shape[0]:
            homecells["proba"] = temp.transpose()

        # Aggregation: residents per homecell (in expectation)
        homecells["n_res"] = homecells.nbpersm * homecells.proba
        homecells = homecells.groupby(["dma_name"]).agg({"n_res": "sum"}).reset_index()
        print(homecells.n_res.sum())
    else:
        homecells = None

    return homecells


def project_residency_to_homecells(key=KEY_WEIGHTING_RAW):
    """
    Project Filosofi residents on home cells and write the result on S3
    Loop over master tiles with projection_residency_to_homecells_master_tile
    """
    filo = get_filosofi(key=KEY_FILOSOFI, drop_tileID=False)
    filo = filo[["x_tile", "y_tile", "nbpersm"]]

    for scheme in ["1", "2", "34"]:

        print("Weighting scheme:")
        print(scheme)

        fv_files = get_FV_files()

        homecells = [
            projection_residency_to_homecells_master_tile(key_fv, filo, scheme=scheme)
            for key_fv in fv_files
        ]
        homecells = pd.concat(homecells)

        homecells = homecells.groupby(["dma_name"]).agg({"n_res": "sum"})
        print("Residents allocated to a home cells:")
        print(homecells.agg({"n_res": "sum"}))
        print("Residents in Filosofi")
        print(filo.nbpersm.sum())

        write_pandas_s3(homecells, key % scheme + ".csv")


def project_marginal_residency_to_homecells(
    key=KEY_REAFFECT_TILES, key_write=KEY_WEIGHTING_RAW
):
    """
    Project marginal* Filosofi residents on home cells and write the result on S3

    *Some Filosofi tiles do not appear in FV coverape map (essentially on coastal areas and borders -
    pointing to a spatial approximation error).

    They are reaffected to the closest tiles in FV coverage map in the file KEY_REFFECT_TILES
    and projected here on home cells
        Args:
        key (str): S3 key with out of FV tiles from Filo grid
        key_write (str): S3 key to write results
    """

    filo = get_filosofi(key=KEY_FILOSOFI, drop_tileID=False)
    filo = filo[["x_tile", "y_tile", "nbpersm"]]

    tiles = get_pandas_s3(KEY_REAFFECT_TILES)
    tiles = tiles.rename(
        {
            "x": "x_tile",
            "y": "y_tile",
            "x_tile": "x_tile_reaffect",
            "y_tile": "y_tile_reaffect",
        },
        axis=1,
    )
    margin = pd.merge(filo, tiles, on=["x_tile", "y_tile"])
    margin = margin[["x_tile_reaffect", "y_tile_reaffect", "nbpersm"]]
    margin = margin.rename(
        {"x_tile_reaffect": "x_tile", "y_tile_reaffect": "y_tile"}, axis=1
    )

    for scheme in ["1", "2", "34"]:

        print("Weighting scheme:")
        print(scheme)

        fv_files = get_FV_files()

        # Exclude large interior files without matching beforehand.
        exclude = (
            [
                KEY_FV_100 + "master_id=350000-" + str(id_y) + "0000/"
                for id_y in range(225, 245, 10)
            ]
            + [
                KEY_FV_100 + "master_id=450000-" + str(id_y) + "0000/"
                for id_y in range(185, 245, 10)
            ]
            + [
                KEY_FV_100 + "master_id=550000-" + str(id_y) + "0000/"
                for id_y in range(185, 255, 10)
            ]
            + [
                KEY_FV_100 + "master_id=650000-" + str(id_y) + "0000/"
                for id_y in range(195, 255, 10)
            ]
            + [
                KEY_FV_100 + "master_id=750000-" + str(id_y) + "0000/"
                for id_y in range(195, 255, 10)
            ]
            + [
                KEY_FV_100 + "master_id=850000-" + str(id_y) + "0000/"
                for id_y in range(195, 215, 10)
            ]
            + [
                KEY_FV_100 + "master_id=850000-" + str(id_y) + "0000/"
                for id_y in range(225, 255, 10)
            ]
        )
        fv_files = [file for file in fv_files if file[0:76] not in exclude]

        homecells = [
            projection_residency_to_homecells_master_tile(key_fv, margin, scheme=scheme)
            for key_fv in fv_files
        ]
        homecells = pd.concat(homecells)

        homecells = homecells.groupby(["dma_name"]).agg({"n_res": "sum"})
        print("Residents allocated to a home cells:")
        print(homecells.agg({"n_res": "sum"}))
        print("Residents in Filo Margin")
        print(margin.nbpersm.sum())

        write_pandas_s3(homecells, key_write % scheme + "_marginal.csv")


def compute_raw_weights(scheme):
    """Compute raw weights at home cell level
    Args:
        scheme (str):  '1', '2' or '34' which stand for all time, nighttime,
        distinct days home detection
    Returns:
        pd.DataFrame: raw weights, home cells and their geography
    """
    mobile = get_home_cells(scheme)
    res = get_residents_on_home_cells(scheme)

    weights = pd.merge(mobile, res, on="dma_name", how="left")
    weights.n_res = weights.n_res.fillna(0)
    weights = weights.rename({"count": "n_imsi"}, axis=1)
    weights["w_raw"] = weights.n_res / weights.n_imsi

    cells = get_cells_point_geography()
    weights = pd.merge(weights, cells, on="dma_name")

    return weights


def group_contiguous_cells(df_antenne, seuil_n_imsi):
    """Create groups of contiguous home cells with
    at least seuil_n_imsi detected resident devices.

    Args:
        pd.DataFrame: df_antenne with columns dma_name (cell identifier),
        x,y (coordinates used for the cell in the grouping task), n_imsi
        (detected resident device on this cell)
    Returns:
        pd.DataFrame with columns gpe_agg (identifier of the cell group),
        and dma_name (identifiers of the cell belonging to the group)
    """

    antenne = df_antenne.copy()
    # Initialize cells group with all cells
    antenne["gpe_agg"] = antenne.dma_name

    # Compute features (coordinates and resident devices) by group of cells
    antenne_gpe_agg = features_gpe_agg(antenne).copy()

    # Search for group of cells under threshold for detected residents
    antenne_pb = antenne_gpe_agg[antenne_gpe_agg.n_imsi < seuil_n_imsi].copy()

    # Loop until no groups are left under threshold
    etape = 0
    while antenne_pb.shape[0] > 0:
        print("reste à traiter : ", antenne_pb.shape[0])

        # Search for pair-wise nearest neighbours
        voisinage = nearest_neighbouring_cell(antenne_pb, antenne_gpe_agg)
        # Create connected components as groups from pairs of neighbours
        agg = neighbours_connected_component(voisinage, etape)

        # Update gpe_agg
        antenne = antenne.merge(agg, on="gpe_agg", how="outer")
        antenne.loc[antenne.gpe.isna(), "gpe"] = antenne.gpe_agg[antenne.gpe.isna()]
        antenne["gpe_agg"] = antenne["gpe"]
        antenne = antenne.drop("gpe", axis=1)

        # Get new groups features for next step
        antenne_gpe_agg = features_gpe_agg(antenne).copy()
        antenne_pb = antenne_gpe_agg[antenne_gpe_agg.n_imsi < seuil_n_imsi].copy()
        etape = etape + 1
    return antenne


def features_gpe_agg(antenne):
    """
    Compute features of group of cells gpe_agg from that of their units:
    coordinates and total detected residents.
    Units may be cells or group of cells.

    Args:
        pd.DataFrame: Rows are units caracterized with columns
        gpe_agg (identifier of the group of units) x,y (coordinates
        used for the grouping task), n_imsi (detected resident devices)
    Returns:
        pd.DataFrame: Rows are group of cells identified
        with column gpe_agg, caracterized with columns
        x,y (coordinates used for the group of cells in the grouping task),
        n_imsi (detected resident devices on the group of cells)
    """
    antenne_gpe_agg = antenne.copy()
    antenne_gpe_agg.x = antenne_gpe_agg.x * antenne_gpe_agg.n_imsi
    antenne_gpe_agg.y = antenne_gpe_agg.y * antenne_gpe_agg.n_imsi
    antenne_gpe_agg = antenne_gpe_agg.groupby("gpe_agg", as_index=False)[
        "x", "y", "n_imsi"
    ].sum()
    antenne_gpe_agg.x = antenne_gpe_agg.x / antenne_gpe_agg.n_imsi
    antenne_gpe_agg.y = antenne_gpe_agg.y / antenne_gpe_agg.n_imsi
    return antenne_gpe_agg


def nearest_neighbouring_cell(antenne_a_regrouper, antenne_cible):
    # Index target group of cells into a KDTree
    tree = KDTree(antenne_cible[["x", "y"]])
    # Query units to group into this index and return the 2 nearest neighbours
    voisinage = tree.query(antenne_a_regrouper[["x", "y"]], k=2, return_distance=False)
    voisinage_antenne = antenne_a_regrouper.copy()
    # Get identifiers of the 2 nearest neighbours
    voisin0 = antenne_cible.gpe_agg[voisinage[:, 0]].values
    voisin1 = antenne_cible.gpe_agg[voisinage[:, 1]].values
    # If nearest neighbour is itself, get second nearest neighbour
    voisin0[voisin0 == antenne_a_regrouper.gpe_agg.values] = voisin1[
        voisin0 == antenne_a_regrouper.gpe_agg.values
    ]
    # Returns per unit the identifier of its affected group
    voisinage_antenne["voisin"] = voisin0
    return voisinage_antenne


def neighbours_connected_component(voisinage, etape):
    """
    Create connected components from pair-wise neighbouring cells
    """
    # Create a symetric adjacency matrix
    voisinage2 = voisinage[["voisin", "gpe_agg"]]
    voisinage2.columns = ["gpe_agg", "voisin"]
    adjacence = pd.concat([voisinage[["gpe_agg", "voisin"]], voisinage2])
    adjacence["un"] = 1
    adjacence = adjacence.groupby(["gpe_agg", "voisin"], as_index=False).count()
    g = nx.convert_matrix.from_pandas_edgelist(
        adjacence[["gpe_agg", "voisin"]], source="gpe_agg", target="voisin"
    )

    # Create the connected component
    composantes = nx.connected_components(g)
    agg = [
        pd.DataFrame({"gpe_agg": list(composante), "gpe": str(etape) + "_" + str(i)})
        for i, composante in enumerate(composantes)
    ]
    agg = pd.concat(agg)
    return agg


def compute_group_weights(scheme):
    """Compute raw weights at group of contiguous home cell level
    Args:
        scheme (str):  '1', '2' or '34' which stand for all time, nighttime,
        distinct days home detection
    Returns:
        pd.DataFrame: detected device (column n_imsi) and detected residents
        (column n_res) at group of contiguous home cell level (column gpe_agg)
    """
    weights = compute_raw_weights(scheme)

    # Get x,y for grouping cell algorithm
    missing = (
        weights.barycentre_antenne_x_unif.isna()
        | weights.barycentre_antenne_y_unif.isna()
    )
    weights.loc[missing, "barycentre_antenne_x_unif"] = weights[missing].COORD_X
    weights.loc[missing, "barycentre_antenne_y_unif"] = weights[missing].COORD_Y

    df_antenne = weights[
        ["dma_name", "barycentre_antenne_x_unif", "barycentre_antenne_y_unif", "n_imsi"]
    ]
    df_antenne.columns = ["dma_name", "x", "y", "n_imsi"]
    df_antenne = df_antenne[~df_antenne.x.isna()]

    # Call grouping cell algorithm
    weights_group = group_contiguous_cells(df_antenne, seuil_n_imsi=20)

    # Get weights component at group of cells level
    weights = pd.merge(
        weights[["dma_name", "n_imsi", "n_res"]],
        weights_group[["dma_name", "gpe_agg"]],
        on="dma_name",
    )
    weights_gpr = weights.groupby("gpe_agg").agg({"n_imsi": "sum", "n_res": "sum"})

    weights_gpr = pd.merge(weights[["dma_name", "gpe_agg"]], weights_gpr, on="gpe_agg")

    return weights_gpr


def compute_bounded_weights(scheme, pct=[0.02, 0.98]):
    """Compute final weights by bounding to percentile p
    Args:
        scheme (str):  '1', '2' or '34' which stand for all time, nighttime, distinct days home detection
        pct (list): [p1, p2] Percentile pair to trim weights
    Returns:
        pd.DataFrame: raw, grouped, and bounded (final) weights
    """

    weights = get_pandas_s3(KEY_WEIGHTING_GROUP % scheme)
    weights["w_gpe"] = weights.n_res_gpe / weights.n_imsi_gpe
    bounds = weights.w_gpe.quantile(pct)

    weights["w_bnded"] = weights.w_gpe
    weights.loc[weights.w_gpe < bounds[pct[0]], "w_bnded"] = bounds[pct[0]]
    weights.loc[weights.w_gpe > bounds[pct[1]], "w_bnded"] = bounds[pct[1]]
    rescale = sum(weights.w_raw * weights.n_imsi) / sum(
        weights.w_bnded * weights.n_imsi
    )
    weights["w_bnded"] = weights.w_bnded * rescale

    return weights


if __name__ == "__main__":

    step = 4

    if step == 1:

        project_residency_to_homecells(KEY_WEIGHTING_RAW)

    if step == 2:

        project_marginal_residency_to_homecells(KEY_REAFFECT_TILES, KEY_WEIGHTING_RAW)

    if step == 3:

        for scheme in ["1", "2", "34"]:

            weights = compute_raw_weights(scheme)
            weights_gpr = compute_group_weights(scheme)
            weights_gpr = weights_gpr.rename(
                {"n_imsi": "n_imsi_gpe", "n_res": "n_res_gpe"}, axis=1
            )

            if weights.shape[0] == weights_gpr.shape[0]:

                weights = pd.merge(weights, weights_gpr, on="dma_name")

                if weights.shape[0] == weights_gpr.shape[0]:

                    print("Writing on S3")
                    write_pandas_s3(weights, KEY_WEIGHTING_GROUP % scheme)

    if step == 4:

        for scheme in ["1", "2", "34"]:

            weights = compute_bounded_weights(scheme)

            if (
                abs(sum(weights.w_bnded * weights.n_imsi) - sum(weights.n_res))
            ) < 0.0001:
                print(sum(weights.n_res))

                print("Writing on S3")
                write_pandas_s3(weights, KEY_WEIGHTING_EVAL % scheme)

                # To DIOD format.
                weights = weights[["dma_name", "w_bnded"]]

                weights["lac"] = weights.dma_name.apply(lambda x: x.split(sep="_")[0])
                weights["ci_sac"] = weights.dma_name.apply(
                    lambda x: x.split(sep="_")[1]
                )
                weights = weights.rename({"w_bnded": "w"}, axis=1)

                write_pandas_s3(weights[["lac", "ci_sac", "w"]], KEY_WEIGHTING % scheme)
