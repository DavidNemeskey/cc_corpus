#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Config loader for the webapp.
# TODO maybe we should extend it beyond the webapp framework?
"""

import yaml


def load_config_file(config_file):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    return config


config = load_config_file("app/config.yaml")
