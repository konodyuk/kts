import os
import shutil
import time

import click

from kts.cli.utils import check_existence, clear_all, list_files, create_config

VALID_EXAMPLES = ['titanic']
KTS_PROJECT_PATHS = ['./input', './notebooks', './storage', './output', './submissions']


@click.group()
def cli():
    pass


@cli.command()
def init():
    """Initialize empty KTS project.

    Create ./input, ./notebooks, ./kts_config.py, etc.
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


@cli.command()
@click.argument('name', type=click.Choice(VALID_EXAMPLES))
def example(name):
    """Download an example to current dir.
    
    Download the example named NAME from github.com/konodyuk/kts-examples repository.
    """
    if os.path.exists(name):
        raise OSError(f'Path ./{name} already exists.')
    download_name = ".kts_examples_" + str(int(time.time()))
    os.system(f"git clone https://github.com/konodyuk/kts-examples {download_name}")
    shutil.move(f"{download_name}/{name}", f"{name}")
    os.system(f"""cd {name} && echo "n\ny\ny" | kts init""")
    shutil.rmtree(f"{download_name}")
