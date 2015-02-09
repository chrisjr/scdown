# Skeleton of a CLI

import click

import scdown


@click.command('scdown')
@click.argument('count', type=int, metavar='N')
def cli(count):
    """Echo a value `N` number of times"""
    for i in range(count):
        click.echo(scdown.has_legs)
