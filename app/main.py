from enum import Enum
from fastapi import FastAPI
from fastapi.exceptions import HTTPException
from pathlib import Path
from pydantic import BaseModel
import subprocess

LOG_DIR = Path("/mnt/d/coding/test_corpus2/logs")

app = FastAPI(
    title = "CC Corpus manager"
)

class Status(Enum):
    PRELAUNCH = "prelaunch"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"

class Step(BaseModel):
    id: int
    script: str
    script_version: str
    comment: str
    status: Status
    # input_data_generated_by: int
    input: Path = None
    output: Path = None
    further_params: list = []

    def run_script(self):
        arguments = [self.script,
                     "-i", self.input,
                     "-o", self.output,
                     "--sid", str(self.id),
                     ]
        arguments += self.further_params
        logfile = LOG_DIR / f"step_{self.id}_{self.script.split('.')[0]}.log"
        with open(logfile, 'w') as log_f:
            subprocess.Popen(arguments, stdout=log_f, stderr=log_f)
        self.status = Status.RUNNING

steps = {
    0: Step(id=0, script="remove_boilerplate.py",
            input=Path("../test_corpus2/04a_index_sorted"),
            output=Path("../test_corpus2/05_boilerplate_removed"),
            further_params=["-w", "../test_corpus2/04_downloaded", "-b", "justext"],
            script_version="1.14.0", comment="", status=Status.PRELAUNCH),
    1: Step(id=1, script="blabla script2", script_version="1.14.0", comment="", status=Status.PRELAUNCH),
    2: Step(id=2, script="blabla script2", script_version="1.14.0", comment="", status=Status.DONE)
}

@app.get("/")
def index() -> dict[str, dict[int, Step]]:
    return {"steps": steps}

@app.get("/step/{step_id}")
def query_step_by_id(step_id: int) -> Step:
    if step_id not in steps:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    return steps[step_id]


@app.get("/steps/")
def query_steps_by_parameters(script: str | None = None,
                              comment: str | None = None,
                              status: Status | None = None,
                              ):
    # This helper functions checks whether a record matches the query
    def check_step(step: Step) -> bool:
        return all(
            (
                script is None or step.script == script,
                comment is None or step.comment == comment,
                status is None or step.status == status,
            )
        )

    selection = [step for step in steps.values() if check_step(step)]
    return{
        "query": {"script": script, "comment": comment},
        "selection": selection
    }

# TODO status is a protected field, automatically set to PRELAUNCH.
@app.post("/")
def add_step(step: Step) -> dict[str, Step]:
    if step.id in steps:
        HTTPException(
            status_code=400, detail=f"Step with {step_id=} already exist."
        )
    steps[step.id] = step
    return {"added": step}

@app.delete("/delete/{step_id")
def delete_step(step_id: int) -> dict[str, Step]:
    if step_id not in steps:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    # TODO check what is step.status. Only if prelaunch should be deleted!
    step = steps.pop(step_id)
    return {"deleted": step}

# TODO using REST for RPC is not the most elegant way to do it.
@app.post("/run/{step_id}")
def run_step(step_id: int) -> dict[str, Step]:
    if step_id not in steps:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    step = steps[step_id]
    if step.status != Status.PRELAUNCH:
        raise HTTPException(
            status_code=403, detail=f"Step with {step_id=} is not ready for execution."
        )
    step.run_script()
    return {"started": step}

@app.post("/completed/{step_id}")
def report_completed(step_id: int) -> dict[str, Step]:
    if step_id not in steps:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    step = steps[step_id]
    if step.status != Status.RUNNING:
        raise HTTPException(
            status_code=403, detail=f"Step with {step_id=} is not running, how can it be completed?"
        )
    step.status = Status.DONE
    return {"completed": step}
