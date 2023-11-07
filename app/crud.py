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
    db_step.script_version = config["version_number"]

    # Fill up the parameters based on the given input or default to config.yaml
    # TODO how should we handle when non-script-specific settings are overwritten?
    settings = config["scripts"][db_step.step_name]
    settings.update(optional_settings)
    # TODO upgrade this when cc dumps and language codes are introduced:
    dir_head = config['folders']['working_dir']
    dir_tail = '/' + config['cc_batch']
    further_params = "-L " + config['runtime_configurations']['log_level']

    # Some steps can run on only one process and thus have no processes param.
    # These steps are marked by 'no_p_param: True' in the config.yaml.
    if not settings.pop('no_p_param', False):
        further_params += ' -P ' + str(config['runtime_configurations']['processes'])

    # Let's go over and process the parameters:
    for key, value in settings.items():
        if key == 'script_file':
            db_step.script_file = value
        elif key == 'input':
            db_step.input = dir_head + value + dir_tail
        elif key == 'output':
            db_step.output = dir_head + value + dir_tail
        elif key == 'hardwired_params':
            further_params += ' ' + value
        else:
            # Most parameters have a simple value:
            if isinstance(value, str):
                further_params += " -" + key + " " + value
            # If the parameter requires special treatment, then it is a dict.
            elif value['is_path']:
                # If it is a path we must append it to the project root dir.
                # and may have to append the current batch to it.
                if value.get('no_batch_in_path'):
                    further_params += " -" + key + " " + dir_head + value[key]
                else:
                    further_params += " -" + key + " " + dir_head + value[key] + dir_tail

    db_step.further_params = further_params
    db.add(db_step)
    db.commit()
    db.refresh(db_step)
    return db_step


def update_step(db: Session, step: schemas.StepUpdate):
    # get existing data from the DB:
    db_step = db.query(models.Step).filter(models.Step.id == step.id).one_or_none()
    if db_step is None:
        return None
    for key, value in vars(step).items():
        setattr(db_step, key, value)
    db.commit()
    db.refresh(db_step)
    return(db_step)


def delete_step_by_id(db: Session, step_id: int):
    db.query(models.Step).filter(models.Step.id == step_id).delete()
    db.commit()
