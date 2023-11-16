#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A CRUD layer for basic interactions with the DB.
"""

from fastapi.encoders import jsonable_encoder
from pydantic.utils import deep_update
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

    print("============CRUD==============")
    print(config)
    print("----")
    print(optional_settings)
    print("----")

    # Fill up the parameters based on the given input or default to config.yaml
    # We need to update a nested dict with another.
    settings = deep_update(config, optional_settings)
    print(settings)
    print("----")

    dir_head = settings['folders']['working_dir']
    dir_tail = '/' + settings['cc_batch']
    further_params = "-L " + settings['runtime_configurations']['log_level']

    # Some steps can run on only one process and thus have no processes param.
    # These steps are marked by 'no_p_param: True' in the config.yaml.
    if not settings["scripts"][db_step.step_name].pop('no_p_param', False):
        further_params += ' -P ' + str(config['runtime_configurations']['processes'])

    # Let's go over and process the parameters:
    for key, value in settings["scripts"][db_step.step_name].items():
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


def get_pipelines(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Pipeline).offset(skip).limit(limit).all()


def get_pipeline_by_id(db: Session, pipeline_id: int):
    return db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()


def create_pipeline(db: Session, pipeline: schemas.PipelineCreate):
    db_pipeline = models.Pipeline(**pipeline.dict())
    db_pipeline.status = "seeded"
    db.add(db_pipeline)
    db.commit()
    db.refresh(db_pipeline)
    return db_pipeline


def update_pipeline(db: Session, pipeline: schemas.PipelineUpdate):
    # get existing data from the DB:
    db_pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline.id).one_or_none()
    if db_pipeline is None:
        return None
    for key, value in vars(pipeline).items():
        setattr(db_pipeline, key, value)
    db.commit()
    db.refresh(db_pipeline)
    return(db_pipeline)


def spawn_pipeline(db: Session, pipeline_id: int):
    db_pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()

    # Spawn the steps belonging to this pipeline:
    config_mod = db_pipeline.params_to_config()
    step_types = config["pipelines"][db_pipeline.template]["steps"]
    step_ids = []
    for step_type in step_types:
        step_name = step_type
        step = schemas.StepCreate(step_name=step_name,
                                  comment=f"Spawned by Pipeline {pipeline_id}")
        db_step = create_step(db, step, config_mod)
        step_ids.append(db_step.id)

    # Save the ids of the newly spawned steps in the pipeline object:
    db_pipeline.steps = jsonable_encoder(step_ids)
    db_pipeline.status = "spawned"
    db.commit()
    return db_pipeline
