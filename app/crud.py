#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A CRUD layer for basic interactions with the DB.
"""

from sqlalchemy.orm import Session
from .config import config
from . import models, schemas


def get_steps(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Step).offset(skip).limit(limit).all()


def get_step_by_id(db: Session, step_id: int):
    return db.query(models.Step).filter(models.Step.id == step_id).first()


def create_step(db: Session,
                step: schemas.StepCreate,
                optional_settings={}
                ):
    db_step = models.Step(**step.dict())
    db_step.status = 'prelaunch'

    # Fill up the parameters based on the given input or default to config.yaml
    settings = config["scripts"][db_step.step_name]
    settings.update(optional_settings)
    # TODO upgrade this when cc dumps and language codes are introduced:
    dir_head = config["folders"]["working_dir"]
    further_params = ""

    # Let's go over and process the parameters:
    for key, value in settings.items():
        if key == 'script_file':
            db_step.script_file = value
        elif key == 'input':
            db_step.input = dir_head + value
        elif key == 'output':
            db_step.output = dir_head + value
        elif key == 'hardwired_params':
            further_params += " " + value
        elif key == 'secondary_input':
            if value:
                further_params += " " + settings.get('secondary_input_param')
                further_params += " " + dir_head + settings.get('secondary_input_dir')
        elif key == 'secondary_input_param' or key == 'secondary_input_dir':
            # We process these when running into the secondary_input key
            continue
        else:
            further_params += " -" + key + " " + value

    db_step.further_params = further_params
    db.add(db_step)
    db.commit()
    db.refresh(db_step)
    return db_step


def delete_step_by_id(db: Session, step_id: int):
    db.query(models.Step).filter(models.Step.id == step_id).delete()
    db.commit()
