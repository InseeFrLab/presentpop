"""
Storing constant.
"""

import os

# Config
BUCKET = "projet-telmob"
ENDPOINT_URL = "https://minio.lab.sspcloud.fr/"
# KEY
KEY_POPULATION = "cancan/population_hourly_tiles_grid50_v01.csv"
KEY_MOBILES = "cancan/mobiles_hourly_tiles_grid50.csv"
KEY_GRILLE_50 = "grilles_reduites/grille50.geojson"
KEY_PROBABILITIES = "02_fv100m_par_carreau_maitre_128km_enrichis/"
KEY_POPULATION_CELL = (
    "cancan/hourly_resident_v1_pop_presente_w1/tables_count_variant_weights/"
)
KEY_FILOSOFI_HOUSEHOLD = "grilles_reduites/filosofi2016_anonyme.csv"
KEY_FILOSOFI = "antennes/cancan/grille_filosofi_2016.csv"
KEY_IMSI_COUNT_CELL = "cancan/mars2019/hourly_imsi_cells_int_not_int.csv"
KEY_JRC = "JRC_estimates_France/data3035_%s_Months%d.csv"
KEY_ACTIVE_MOBILES_OLD = "cancan/active_mobiles_hourly_tiles_grid50.csv"
KEY_AREA_ATTRACTION = "segmentation/zMetro.%s"
KEY_HEALTHCARE = "territoiresSante/services-de-reanimation.geojson"
KEY_IMSI_COUNT_CELL_SCOPE_PARQUET = (
    "cancan/mars2019/hourly_imsi_cells_no_int/parquets/density_imsiV4_bis/03/"
)
KEY_IMSI_COUNT_CELL_INT_SCOPE_PARQUET = (
    "cancan/mars2019/hourly_imsi_cells/parquets/density_imsiV4/03/"
)
KEY_IMSI_COUNT_CELL_OLD = "cancan/mars2019/hourly_FRPhone_imsi_cells_all_france.csv/"

# Key For Spatial Mapping.
KEY_SM_FV = "antennes/cancan/ALLCELLSDT_with_alternative_from_FV_updated.csv"
KEY_SM_FV_grid = "grilles_reduites/antenna_to_grid_50_uniform_prior.csv"
KEY_SM_noFV_grid = "antennes/cancan/CELLS_without_alternative_from_FV_togrid50.csv"
KEY_FV_100 = "02_fv100m_par_carreau_maitre_100km_enrichis/"
KEY_FV_BARYCENTER = "01b_antennes_barycentres.csv"


# KEY FOR WEIGHTING
KEY_HOME_CELLS = "cancan/weighting/weighting%scsv"
KEY_WEIGHTING_RAW = "cancan/weighting/residents_in_cells_w%s"
KEY_REAFFECT_TILES = (
    "antennes/cancan/traitement_carreaux_non_couverts_par_antenne_2016.csv"
)
KEY_WEIGHTING_GROUP = "cancan/weighting/weighting_grouped_%s.csv"
KEY_WEIGHTING_EVAL = "cancan/weighting/weighting_eval_files_%s.csv"
KEY_WEIGHTING = "cancan/weighting/weighting%s.csv"

# KEY FOR STATISTICS
KEY_AGG_INTERPOLATION_ERROR = "cancan/mars2019/interpolation_error.csv"

POSTGRESQL_HOST = "postgresql-908083"
POSTGRESQL_USER = "projet-telmob"
POSTGRESQL_PASSWORD = "6oiyh8ufhkscbbkwflwk"
POSTGRESQL_PORT = "5432"

# KEY FOR RETRIEVING DATA ENCRYPTION KEY
VAULT_PATH = "projet-telmob/mobitic"
VAULT_KEY = "CLE_CHIFFREMENT"

PATH_MARATHON = os.path.dirname(__file__) + "/marathon.json"
