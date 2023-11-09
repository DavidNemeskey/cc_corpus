#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Defines the model classes.
These implements the SQLAlchemy ORM and also the model-logic level methods.
"""

from sqlalchemy import Column, Integer, String

# from sqlalchemy.dialects.postgresql import JSON  # For PostgreSQL
# from sqlalchemy.dialects.mysql import JSON  # For MySQL
from sqlalchemy import JSON  # For SQLite

from pathlib import Path
import subprocess

from .config import config
from .database import Base


STEP_STATUSES = ["prelaunch", "running", "completed"]
PIPELINE_STATUSES = ["seeded", "spawned"]


class Step(Base):
    __tablename__ = "steps"

    id = Column(Integer, primary_key=True, index=True)
    step_name = Column(String)
    script_file = Column(String)
    script_version = Column(String)
    comment = Column(String)
    status = Column(String)
    input = Column(String)
    output = Column(String)
    further_params = Column(String)

    def run_script(self):
        """
        Starts the actual execution of the script defined by this step.

        Passes the name of this script and all its runtime parameters to the
        api_wrapper script and starts that as a separate process.
        This is a "fire and forget" launch. It is the responsibility of the
        api_wrapper.py to wait for the completion of the actual task and make
        a callback to the API.
        """
        arguments = ["api_wrapper.py",
                     str(self.id),
                     self.script_file,
                     "-o", self.output,
                     ]
        if self.input:
            arguments.append("-i")
            arguments.append(self.input)
        arguments += self.further_params.split()
        print(f"Executing script: {arguments}")
        log_dir = Path(config["folders"]["logs"])
        logfile = log_dir / f"step_{self.id}_{self.script_file.split('.')[0]}.log"
        with open(logfile, 'w') as log_f:
            subprocess.Popen(arguments, stdout=log_f, stderr=log_f)


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(Integer, primary_key=True, index=True)
    comment = Column(String)
    status = Column(String)
    template = Column(String)
    params = Column(JSON)
    steps = Column(JSON)
