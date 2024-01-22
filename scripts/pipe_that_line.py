#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Creates a pipeline and its constituent steps in the manager app DB
and tries to execute them using the autorunner feature of the app.
"""

from argparse import ArgumentParser
import logging

from app.config import config
import app.crud as crud
import app.database as database
import app.schemas as schemas


def possible_pipeline_params():
    """Collects the possible parameters for pipelines from app/config.yaml"""
    ppp = set()
    pipes = config["pipelines"]
    for pipe in pipes.values():
        ppp.update(pipe["params"])
    return ppp


def parse_arguments():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('pipe', help='the input directory.')
    parser.add_argument('--log-level', '-L', type=str, default='info',
                        choices=['debug', 'info', 'warning',
                                 'error', 'critical'],
                        help='the logging level.')

    # The parameters we accept are fetched from the config.yaml:
    ppp = possible_pipeline_params()
    for param in ppp:
        parser.add_argument(f'--{param}',
                            help='A parameter for a pipeline')

    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(threadName)-10s)- %(levelname)s - %(message)s'
    )

    # Get the args arranged into pipeline parameters:
    try:
        pipe_params = config["pipelines"][args.pipe]["params"]
        print(pipe_params)
    except KeyError:
        logging.error(f"Pipeline type {args.pipe} was not found in the "
                      f"config files.")
    params = {}
    for param in pipe_params:
        param_value = getattr(args, param, None)
        if param_value:
            params[param] = param_value
        else:
            logging.error(f"Pipeline type {args.pipe} requires parameter "
                          f"{param} but it was not supplied.")
            raise KeyError

    logging.info(f"Creating pipeline type {args.pipe} with the following "
                 f"parameters: {params}")
    pipeline = schemas.PipelineCreate(
        template=args.pipe,
        params=params,
    )
    print(pipeline)
    # Get the DB connection:
    db = database.SessionLocal()
    # Create the pipeline object:
    db_pipeline = crud.create_pipeline(db, pipeline)
    # Spawn its corresponding steps:
    crud.spawn_pipeline(db, db_pipeline.id)
    # Set pipeline status for autorun, otherwise it will skip this Pipeline.
    db_pipeline.status = "autorun"
    crud.update_pipeline(db, db_pipeline)
    # Note: if there were already Pipelines waiting for autorun execution
    # our newly created pipeline will be at the end of the queue.
    crud.autorun_pipelines(db)
    db.close()


if __name__ == '__main__':
    main()
