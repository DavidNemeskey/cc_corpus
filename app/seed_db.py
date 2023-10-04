#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Methods responsible for seeding the DB for development purposes.
Called by the main.py#dev_seed() method if the DB is empty.
"""

from sqlalchemy.orm import Session

from . import models


def seed_steps(db: Session):
    steps = [
        {
            "script": "remove_boilerplate.py",
            "input": "../test_corpus2/04a_index_sorted",
            "output": "../test_corpus2/05_boilerplate_removed",
            "further_params":
                "-w ../test_corpus2/04_downloaded -b justext",
            "script_version": "1.14.0",
            "comment": "",
            "status": "prelaunch"
        },
    ]
    for step in steps:
        db.add(models.Step(**step))
    db.commit()
