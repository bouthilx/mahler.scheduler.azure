# -*- coding: utf-8 -*-
"""
:mod:`mahler.scheduler.flow -- TODO
===================================

.. module:: flow
    :platform: Unix
    :synopsis: TODO

TODO: Write long description
"""
import os

import mahler.core
import mahler.core.utils.config

from ._version import get_versions
from .resources import FlowResources

VERSIONS = get_versions()
del get_versions

__descr__ = 'TODO'
__version__ = VERSIONS['version']
__license__ = 'GNU GPLv3'
__author__ = u'Xavier Bouthillier'
__author_short__ = u'Xavier Bouthillier'
__author_email__ = 'xavier.bouthillier@umontreal.ca'
__copyright__ = u'2018, Xavier Bouthillier'
__url__ = 'https://github.com/bouthilx/mahler.scheduler.flow'

DEF_CONFIG_FILES_PATHS = [
    os.path.join(mahler.core.DIRS.site_data_dir, 'scheduler', 'flow', 'config.yaml.example'),
    os.path.join(mahler.core.DIRS.site_config_dir, 'scheduler', 'flow', 'config.yaml'),
    os.path.join(mahler.core.DIRS.user_config_dir, 'scheduler', 'flow', 'config.yaml')
    ]


def build(max_workers, **kwargs):
    return FlowResources(max_workers=max_workers)


def build_parser(parser):
    """Return the parser that needs to be used for this command"""
    flow_parser = parser.add_parser('flow', help='flow help')

    flow_parser.add_argument(
        '--max-workers', type=int,
        help='number of concurrent workers to submit.')


def define_config():
    config = mahler.core.utils.config.Configuration()
    config.add_option(
        'max_workers', type=int, default=1, env_var='MAHLER_SCHEDULER_FLOW_MAX_WORKERS')

    return config


def parse_config_files(config):
    mahler.core.utils.config.parse_config_files(
        config, mahler.core.DEF_CONFIG_FILES_PATHS,
        base='scheduler.flow')

    mahler.core.utils.config.parse_config_files(
        config, DEF_CONFIG_FILES_PATHS)
