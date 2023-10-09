#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Definitions of pydantic models.

FastAPI uses these pydantic models to process JSON inputs and outputs.
Also used for data validation and to generate meaningful error messages.
We call the pydantic models schemas to avoid naming conflict with SQLAlchemy
ORM models (which are defined in models.py).
"""

from pydantic import BaseModel


class StepBase(BaseModel):
    step_name: str
    comment: str = None
    # input_data_generated_by: int


class StepCreate(StepBase):
    pass


class Step(StepBase):
    id: int
    script_file: str
    script_version: str
    input: str
    output: str
    further_params: str
    status: str

    class Config:
        orm_mode = True
