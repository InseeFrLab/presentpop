"""Console script for mobitic_utils."""
import os
import sys
import click
import requests
import json

from sqlalchemy import create_engine
from mobitic_utils.export_config import EXPORT_CONFIG
from mobitic_utils.constants import (
    PATH_MARATHON,
)
from mobitic_utils.db import check_column_exist

engine = create_engine(
    f"postgresql://{os.environ['POSTGRESQL_USER']}:{os.environ['POSTGRESQL_PASSWORD']}\
@{os.environ['POSTGRESQL_HOST']}:{os.environ['POSTGRESQL_PORT']}/defaultdb"
)


def _print_json(_json: dict):
    click.echo(json.dumps(_json, indent=2, sort_keys=True))


@click.group()
def main(args=None):
    """Console script for mobitic_utils."""
    return 0


@main.command(help="Export average week of population to db")
def init_db():

    for data in EXPORT_CONFIG:
        click.echo(f"\U0001f6eb exporting: {data['name']}")
        # If table don't exist, Create.
        if data["type"] == "join":
            if not check_column_exist(data["table 1"], data["column"]):
                data["export"](data["table 1"], data["table 2"])
                click.echo(f"\U0001f389 Done exporting: {data['name']}!")
            else:
                click.echo(f"\U0001f63a {data['name']} already exported!")
        else:
            if (
                not engine.dialect.has_table(engine.connect(), data["table"])
                or data["force"]
            ):
                data["export"](data["table"])
                click.echo(f"\U0001f389 Done exporting: {data['name']}!")
            else:
                click.echo(f"\U0001f63a {data['name']} already exported!")


@main.command(help="Create empty db in marathon")
def deploy_empty_db():
    with open(PATH_MARATHON) as f:
        data = json.load(f)
    _print_json(
        requests.put(
            url="http://marathon.mesos:8080/v2/apps/postgis-mobitic?force=true",
            json=data,
            headers={"Content-Type": "application/json"},
        ).json()
    )
    _print_json(
        requests.post(
            url="http://marathon.mesos:8080/v2/apps/postgis-mobitic/restart?force=true"
        ).json()
    )


@main.command(help="Check db in marathon")
def check_db():
    _print_json(
        requests.get(url="http://marathon.mesos:8080/v2/apps/postgis-mobitic").json()
    )


@main.command(help="Test function")
def test():
    pass


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
