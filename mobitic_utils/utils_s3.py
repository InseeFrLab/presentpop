import boto3
import io
import hvac
import os
import pandas as pd
import geopandas as gpd
from mobitic_utils.constants import ENDPOINT_URL, BUCKET, VAULT_PATH, VAULT_KEY

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL)
client = hvac.Client(url=os.environ["VAULT_ADDR"], token=os.environ["VAULT_TOKEN"])
encryptFiles = client.secrets.kv.read_secret_version(
    path=VAULT_PATH, mount_point="onyxia-kv"
)


def get_pandas_s3(Key: str) -> pd.DataFrame:
    """Get a pandas DataFrame from an S3 Key"""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=Key)
    except:
        obj = s3.get_object(
            Bucket=BUCKET,
            Key=Key,
            SSECustomerKey=encryptFiles["data"]["data"][VAULT_KEY],
            SSECustomerAlgorithm="AES256",
        )
    df = pd.read_csv(io.BytesIO(obj["Body"].read()), index_col=0)

    return df


def get_geopandas_s3(Key: str, epsg: int) -> gpd.GeoDataFrame:
    """Get a geo pandas DataFrame from an S3 Key"""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=Key)
    except:
        obj = s3.get_object(
            Bucket=BUCKET,
            Key=Key,
            SSECustomerKey=encryptFiles["data"]["data"][VAULT_KEY],
            SSECustomerAlgorithm="AES256",
        )
    df = gpd.read_file(io.BytesIO(obj["Body"].read()), index_col=0)
    df = df.set_crs(epsg=epsg, allow_override=True)

    return df


def write_pandas_s3(df: pd.DataFrame, Key: str):
    """Write a pandas DataFrame to S3 from an S3 Key"""

    buff = io.StringIO()
    df.to_csv(buff)
    buff2 = io.BytesIO(buff.getvalue().encode())
    s3.upload_fileobj(buff2, BUCKET, Key)


def get_shapefile(Key: str):
    """Get a shapefile object from S3 Key using string formatting"""

    for ext in ["dbf", "prj", "shx", "shp"]:
        try:
            s3.download_file(BUCKET, Key % ext, f"/tmp/temp.{ext}")
        except:
            s3.download_file(
                BUCKET,
                Key % ext,
                f"/tmp/temp.{ext}",
                ExtraArgs={
                    "SSECustomerKey": encryptFiles["data"]["data"][VAULT_KEY],
                    "SSECustomerAlgorithm": "AES256",
                },
            )

    return gpd.read_file("/tmp/temp.shp")
