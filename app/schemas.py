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
from typing import Dict, List, Optional


class StepBase(BaseModel):
    step_name: str
    comment: Optional[str]


class StepUpdate(BaseModel):
    id: Optional[int]
    step_name: Optional[str]
    comment: Optional[str]
    script_file: Optional[str]
    script_version: Optional[str]
    input: Optional[str]
    output: Optional[str]
    further_params: Optional[str]
    status: Optional[str]

    class Config:
        orm_mode = True


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


class PipelineBase(BaseModel):
    comment: Optional[str]
    template: str

    class Config:
        orm_mode = True


class PipelineCreate(PipelineBase):
    params: Dict
    prereq_pipe: Optional[int]
    prereq_step: Optional[int]


class PipelineUpdate(BaseModel):
    id: Optional[int]
    comment: Optional[str]
    params: Optional[Dict]
    prereq_pipe: Optional[int]
    prereq_step: Optional[int]
    status: Optional[str]
    steps: Optional[List]
    template: Optional[str]

    class Config:
        orm_mode = True


class Pipeline(PipelineBase):
    id: int
    params: Dict
    steps: Dict

    class Config:
        orm_mode = True
