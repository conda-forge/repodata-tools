import time

import click


@click.command()
def main():
    """Worker process for continuously building repodata"""
    while True:
        print("BUILT REPODATA", flush=True)
        time.sleep(30)
