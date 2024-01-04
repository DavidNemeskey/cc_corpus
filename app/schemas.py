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
    """Base model for Steps with the bare minimum fields."""
    step_name: str
    comment: Optional[str]


class StepUpdate(BaseModel):
    """The model for updating a Step."""
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
    """The model for creating a Step, derived from StepBase."""
    pass


class Step(StepBase):
    """The fields used when a Step object is rendered as a response."""
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
    """Base model for Pipelines with the bare minimum fields."""
    comment: Optional[str]
    template: str

    class Config:
        orm_mode = True


class PipelineCreate(PipelineBase):
    """The model for creating a Pipeline."""
    params: Dict
    prereq_pipe: Optional[int]
    prereq_step: Optional[int]


class PipelineUpdate(BaseModel):
    """The model for updating a Pipeline."""
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
    """
    The fields used when a Pipeline object is rendered as a response.
    Not in use currently.
    """
    id: int
    params: Dict
    steps: Dict

    class Config:
        orm_mode = True
