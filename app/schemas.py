from pydantic import BaseModel


class StepBase(BaseModel):
    script: str
    script_version: str
    comment: str
    # input_data_generated_by: int
    input: str
    output: str
    further_params: str


class StepCreate(StepBase):
    pass


class Step(StepBase):
    id: int
    status: str

    class Config:
        orm_mode = True
