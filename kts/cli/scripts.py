import os

import click

from kts.cli.utils import check_existence, clear_all, list_files, create_config

KTS_PROJECT_PATHS = ['./input', './notebooks', './storage', './output', './submissions']


@click.group()
def cli():
    pass


@cli.command()
def init():
    """Initialize empty KTS project.

    Create ./input, ./notebooks, ./kts_config.toml, etc.
    """
    if check_existence(KTS_PROJECT_PATHS):
        list_files('./')
        if click.confirm('Do you want to clear existing kts file system?'):
            clear_all()

    click.confirm('Do you want to build the file system?', abort=True)
    for path in KTS_PROJECT_PATHS:
        if not os.path.isdir(path):
            os.makedirs(path)

    if os.path.exists('./kts_config.py'):
        if click.confirm('Config found. Overwrite?'):
            create_config()
    else:
        create_config()
