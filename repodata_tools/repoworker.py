import time

import click


@click.command()
def main():
    """Worker process for continuously building repodata"""
    while True:
        time.sleep(30)
