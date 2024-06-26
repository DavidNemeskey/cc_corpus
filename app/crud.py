#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A CRUD layer for basic interactions with the DB.
"""
import logging

from fastapi.encoders import jsonable_encoder
from pathlib import Path
from sqlalchemy.orm import Session
import time
from typing import Dict, Union

from .config import config, load_and_substitute_config, CONFIG_FILE
from .logging import configure_logging
from . import models, schemas

configure_logging()


def get_steps(db: Session,
              skip: int = 0,
              limit: int = 100,
              filters: dict[str, str] = None):
    """Gets steps from the DB."""
    if filters:
        return db.query(models.Step).filter_by(**filters).offset(skip).limit(limit).all()
    else:
        return db.query(models.Step).offset(skip).limit(limit).all()


def get_step_by_id(db: Session, step_id: int):
    """Gets a single step by ID from the DB."""
    return db.query(models.Step).filter(models.Step.id == step_id).first()


def generate_path_from_param(param_key: str,
                             param_value: Union[str, Dict[str, str]],
                             dir_head: str,
                             dir_tail: str) -> str:
    if isinstance(param_value, str):
        # This is a simple input field.
        path = dir_head + param_value + dir_tail
    elif param_value.get("no_batch_in_path"):
        path = dir_head + param_value[param_key]
    else:
        raise (ValueError(f"In the config.yaml the input was "
                          f"in an unknown structure"))
    return path


def create_step(db: Session,
                step: schemas.StepCreate,
                optional_settings={}):
    """
    Creates a step in the DB with a lot of data processing.

    Combines the optional_settings supplied in parameter with the config .yaml.
    It combines these parameters into properly formatted values for fields.
    """
    # If we have optional settings, we load the config and substitute those.
    if optional_settings:
        settings = load_and_substitute_config(CONFIG_FILE, optional_settings)
    else:
        # Otherwise we just use the default config:
        settings = config

    db_step = models.Step(**step.dict())
    db_step.status = "prelaunch"
    db_step.script_version = settings["version_number"]

    dir_head = str(Path(settings["folders"]["working_dir"]).expanduser()) + "/"
    dir_tail = "/" + settings["cc_batch"]
    further_params = "-L " + settings["runtime_configurations"]["log_level"]

    # Some steps can run on only one process and thus have no processes param.
    # These steps are marked by "no_p_param: True" in the config.yaml.
    if not settings["scripts"][db_step.step_name].pop("no_p_param", False):
        further_params += " -P " \
                          + str(config["runtime_configurations"]["processes"])

    # Let's go over and process the parameters:
    for key, value in settings["scripts"][db_step.step_name].items():
        if key == "script_file":
            db_step.script_file = value
        elif key == "input":
            db_step.input = generate_path_from_param(key, value, dir_head,
                                                     dir_tail)
        elif key == "output":
            db_step.output = generate_path_from_param(key, value, dir_head,
                                                      dir_tail)
        elif key == "hardwired_params":
            further_params += " " + value
        else:
            # Most parameters have a simple value:
            if isinstance(value, str):
                further_params += " -" + key + " " + value
            # If the parameter requires special treatment, then it is a dict.
            elif value["is_path"]:
                # If it is a path we must append it to the project root dir.
                # and may have to append the current batch to it.
                if value.get("no_batch_in_path"):
                    further_params += " -" + key + " " + dir_head + value[key]
                elif value.get("url_repo_path"):
                    # In this case we don't use the project root dir, we just
                    # use the url repository dir.
                    further_params += " -" + key + " " \
                                      + settings["folders"]["url_repository"]
                elif value.get("minhash_repo_path"):
                    # In this case we don't use the project root dir, we just
                    # use the minhash repository dir.
                    further_params += " -" + key + " " \
                                      + settings["folders"]["minhash_repository"]
                elif value.get("url_repo_append"):
                    # In this case we append the filename to the url_repository
                    # path.
                    print(value)
                    further_params += " -" + key + " " \
                                      + settings["folders"]["url_repository"] \
                                      + value[key]
                else:
                    further_params += " -" + key \
                                      + " " + dir_head + value[key] + dir_tail

    db_step.further_params = further_params
    db.add(db_step)
    db.commit()
    return db_step


def update_step(db: Session, step: schemas.StepUpdate):
    """
    Updates a Step in the DB.

    Fields that are missing from the update object are left unchanged.
    """
    # get existing data from the DB:
    db_step = db.query(models.Step).filter(
        models.Step.id == step.id).one_or_none()
    if db_step is None:
        return None
    # Do the updates:
    for key, value in vars(step).items():
        setattr(db_step, key, value)
    db.commit()
    db.refresh(db_step)
    return(db_step)


def delete_step_by_id(db: Session, step_id: int):
    """Deletes a STEP from the DB."""
    db.query(models.Step).filter(models.Step.id == step_id).delete()
    db.commit()


def get_pipelines(db: Session,
                  skip: int = 0,
                  limit: int = None,
                  status: str = None):
    """Fetches Pipeline objects from the DB."""
    if status:
        return db.query(models.Pipeline).filter(
            models.Pipeline.status == status).offset(skip).limit(
            limit).all()
    else:
        return db.query(models.Pipeline).offset(skip).limit(limit).all()


def is_pipe_ready(db: Session, pipeline_id):
    """
    Checks if a pipeline is ready for autorunner functionallity.
    It returns True if:
    - It is  set to "autorun" status.
    - Its prerequisites are completed.
    """
    pipe = get_pipeline_by_id(db, pipeline_id)
    if pipe.status != "autorun":
        return False
    # If there are no (well-defined) prerequisites, then pipe is ready:
    if pipe.prereq_pipe is None:
        return True
    if pipe.prereq_step is None:
        return True
    prereq_pipe = get_pipeline_by_id(db, pipe.prereq_pipe)
    # If the prereq pipe has not spawned its steps then it is not ready:
    if len(prereq_pipe.steps) < pipe.prereq_step:
        return False
    prereq_step = prereq_pipe.steps[pipe.prereq_step - 1]
    db_step = get_step_by_id(db, prereq_step)
    if db_step.status == "completed":
        return True
    else:
        return False


def get_pipeline_by_id(db: Session, pipeline_id: int):
    """Fetches a Pipeline object from the DB by ID."""
    return db.query(models.Pipeline).\
        filter(models.Pipeline.id == pipeline_id).one_or_none()


def create_pipeline(db: Session, pipeline: schemas.PipelineCreate):
    """Saves a Pipeline object as a new record."""
    db_pipeline = models.Pipeline(**pipeline.dict())
    db_pipeline.status = "seeded"
    db.add(db_pipeline)
    db.commit()
    db.refresh(db_pipeline)
    return db_pipeline


def update_pipeline(db: Session, pipeline: schemas.PipelineUpdate):
    """
    Updates a Pipeline in the DB
    Fields that are missing from the update object are left unchanged.
    """
    # get existing data from the DB:
    db_pipeline = get_pipeline_by_id(db, pipeline.id)
    if db_pipeline is None:
        return None
    # Do the updates:
    for key, value in vars(pipeline).items():
        setattr(db_pipeline, key, value)
    db.commit()
    db.refresh(db_pipeline)
    return(db_pipeline)


def spawn_pipeline(db: Session, pipeline_id: int):
    """
    Spawns the steps corresponding to the pipeline.

    The config.yaml describes what steps should be created for which type of
    pipeline.
    Their fields are also determined based on the config.yaml and the
    parameters of their mother Pipeline object.
    The ids of its Steps are stored in the steps field of the Pipeline.
    The Pipeline object's status is changed to "spawned".
    """
    db_pipeline = get_pipeline_by_id(db, pipeline_id)

    # Spawn the steps belonging to this pipeline:
    step_types = config["pipelines"][db_pipeline.template]["steps"]
    step_ids = []
    for step_type in step_types:
        step_name = step_type
        step = schemas.StepCreate(step_name=step_name,
                                  comment=f"Spawned by Pipeline {pipeline_id}")
        db_step = create_step(db, step, db_pipeline.params)
        step_ids.append(db_step.id)

    # Save the ids of the newly spawned steps in the pipeline object:
    db_pipeline.steps = jsonable_encoder(step_ids)
    db_pipeline.status = "spawned"
    db.commit()
    return db_pipeline


def get_steps_of_pipeline(db: Session, pipeline_id: int):
    """Fetches the Steps corresponding to the Pipeline."""
    db_pipeline = db.query(models.Pipeline).\
        filter(models.Pipeline.id == pipeline_id).first()
    steps = []
    for step_id in db_pipeline.steps:
        steps.append(get_step_by_id(db, step_id))
    return steps


def autorun_pipelines(db: Session, app_url: str):
    """
    Runs the next step according to the autorun logic.

    - Only Pipelines with the status "autorun" are considered.
    - Pipelines are then ordered by their ids.
    - Once a candidate Pipeline has been selected we examine its Steps:
    -- We skip steps which are "completed".
    -- If we find a step already "running" then the autorunner process waits
    for its completion. If it succeeds it is skipped. If it fails then the
    Pipeline itself is skipped for now.
    -- If we find a step marked as "failed" we skip this Pipeline.
    -- If we get to a "prelaunch" status step, we execute that and wait
    for its resolution (success or failure) and handle it accordingly.
    -- If we are either done with the Steps of a given Pipeline or found a
    failed one, we proceed to the next Pipeline.

    If the last Step of a Pipeline is completed, then we change the Pipeline!s
    status from "autorun" to "completed".
    """
    pipes = get_pipelines(db, status="autorun")
    for pipe in pipes:
        logging.info(f"Attempting to progress pipeline #{pipe.id}")
        if is_pipe_ready(db, pipe.id):
            steps = get_steps_of_pipeline(db, pipe.id)
            encountered_failure = False
            for step in steps:
                if step.status == "completed":
                    logging.info(f"--Step #{step.id} was already completed.")
                    pass
                elif step.status == "running":
                    logging.info(f"--Step #{step.id} was already started.")
                    # We have to wait until it finishes
                    while step.status == "running":
                        logging.info(f"--Waiting for #{step.id} to complete.")
                        time.sleep(10)
                        db.refresh(step)
                    if step.status == "failed":
                        logging.info(f"--Step #{step.id} failed.")
                        encountered_failure = True
                        break
                elif step.status == "failed":
                    # We cannot progress with this pipeline:
                    logging.info(f"--Step #{step.id} failed.")
                    encountered_failure = True
                    break
                else:
                    # We have to run this step:
                    step.run_script(app_url)
                    step.status = "running"
                    db.commit()
                    logging.info(f"--Started the execution of step #{step.id}")
                    # We have to wait until it finishes:
                    while step.status == "running":
                        logging.info(f"--Waiting for #{step.id} to complete.")
                        time.sleep(10)
                        db.refresh(step)
                    if step.status == "failed":
                        logging.info(f"--Step #{step.id} failed.")
                        encountered_failure = True
                        break
            if encountered_failure:
                logging.info(f"Pipeline #{pipe.id} has a failed task.")
            else:
                pipe.status = "completed"
                db.commit()
                logging.info(f"Pipeline #{pipe.id} was completed.")
    logging.info("Autorunner finished.")
