from pathlib import Path
from pydantic import BaseModel


class StepBase(BaseModel):
    script: str
    script_version: str
    comment: str
    status: str
    # input_data_generated_by: int
    input: Path = None
    output: Path = None
    further_params: list = []


class StepCreate(StepBase):
    pass


class Step(StepBase):
    id: int

    class Config:
        orm_mode = True
