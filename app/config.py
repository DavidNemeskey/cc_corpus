#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Config loader for the webapp.
# TODO maybe we should extend it beyond the webapp framework?
"""

from pathlib import Path
from string import Template
from typing import Any, Dict
import yaml


CONFIG_FILE = "app/config.yaml"


def load_config_file(config_file) -> Dict[str, Any]:
    """Loads the config from a YAML file to an object."""
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # If the working dir is not set properly we raise an error:
    working_dir_var = config["folders"]["working_dir"]
    if not Path(working_dir_var).is_dir():
        raise FileNotFoundError(
            f"The working dir {working_dir_var} is missing.")

    return config


def load_and_substitute_config(config_file, variables_dict) -> Dict[str, Any]:
    """
    Loads the config from a YAML file and substitutes variables.
    The variables in the YAML are written as "$key" or "${key}".
    The variables_dict contains these keys and values for them.
    """
    with open(config_file, "r") as f:
        template = Template(f.read())
        content = template.safe_substitute(variables_dict)
        config = yaml.safe_load(content)
    return config


def load_config_with_defaults(config_file) -> Dict[str, Any]:
    """
    Loads the config from a YAML file using the default variables.
    The default variables are themselves contained in that YAML.
    """
    raw_config = load_config_file(config_file)
    default_variables = raw_config["default_yaml_variables"]
    config = load_and_substitute_config(config_file, default_variables)
    return config


def get_logs_dir(config) -> Path:
    logs_path = Path(config["folders"]["logs"])
    if logs_path.is_absolute():
        return logs_path
    else:
        return Path(config["folders"]["working_dir"]) / logs_path


config = load_config_with_defaults(CONFIG_FILE)
