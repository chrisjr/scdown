from __future__ import absolute_import

import click

import logging

from scdown.tasks import process_user
from scdown.sc import prefill_user

logging.basicConfig(level=logging.INFO)

mode = "download"


@click.command('scdown')
@click.argument('user_id', type=int, metavar='N')
def cli(user_id):
    """Process a user by id"""
    if mode == "precache":
        prefill_user(user_id)
    else:
        process_user(user_id)
