# Import household counts at location (x,y)
# Project in a 100m x 100m grid.

import boto3
import io
import os
import pandas as pd
import numpy as np
from pyproj import Transformer


from mobitic_utils.constants import (
    ENDPOINT_URL,
    BUCKET,
    KEY_FILOSOFI_HOUSEHOLD,
    KEY_FILOSOFI,
)

from mobitic_utils.utils_s3 import write_pandas_s3, get_pandas_s3

if __name__ == "__main__":

    s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)

    obj = s3.get_object(Bucket=BUCKET, Key=KEY_FILOSOFI_HOUSEHOLD)
    filosofi2016 = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        compression="gzip",
        dtype={"DEPCOM": str, "x": np.float64, "y": np.float64, "nbpersm": np.float64},
    )

    filosofi2016 = filosofi2016[
        filosofi2016.DEPCOM.str.slice(0, 2) < "96"
    ]  # remove overseas departments

    transformer = Transformer.from_crs("epsg:2154", "epsg:27572")
    filosofi2016.x, filosofi2016.y = transformer.transform(
        filosofi2016.x.tolist(), filosofi2016.y.tolist()
    )  # reproject coordinates

    filosofi2016["x_tile"] = (np.floor((filosofi2016.x) / 100) * 100).astype(int)
    filosofi2016["y_tile"] = (np.floor((filosofi2016.y) / 100) * 100).astype(int)
    filosofi2016_grid = filosofi2016.groupby(["x_tile", "y_tile"], as_index=False)[
        "nbpersm"
    ].sum()
    write_pandas_s3(filosofi2016_grid, KEY_FILOSOFI)
