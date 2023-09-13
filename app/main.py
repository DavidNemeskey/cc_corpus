from fastapi import Depends, FastAPI
from fastapi.exceptions import HTTPException
from sqlalchemy.orm import Session


from . import models, schemas
from .database import SessionLocal, engine



# TODO this should be replaced by proper migrations using the Alembic library
models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title = "CC Corpus manager"
)

# DB Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()





@app.on_event("startup")
def seed_db():
    db = SessionLocal()
    num_steps = db.query(models.Step).count()
    if num_steps == 0:
        steps = [
            {"script": "remove_boilerplate.py",
             "input": "../test_corpus2/04a_index_sorted",
             "output": "../test_corpus2/05_boilerplate_removed",
             "further_params": "-w ../test_corpus2/04_downloaded -b justext",
             "script_version": "1.14.0",
             "comment": "",
             "status": "prelaunch"
            },
        ]
        for step in steps:
            db.add(models.Step(**step))
        db.commit()
    else:
        print(f"We already have {num_steps} records in our DB")

@app.get("/")
def index(db: Session = Depends(get_db)):
    steps = db.query(models.Step).all()
    return {"steps": steps}

@app.get("/step/{step_id}")
def query_step_by_id(step_id: int, db: Session = Depends(get_db)):
    db_step = db.query(models.Step).filter(models.Step.id == step_id).first()
    if not db_step:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    return db_step

#
#
# @app.get("/steps/")
# def query_steps_by_parameters(script: str | None = None,
#                               comment: str | None = None,
#                               status: str | None = None,
#                               ):
#     # This helper functions checks whether a record matches the query
#     def check_step(step: Step) -> bool:
#         return all(
#             (
#                 script is None or step.script == script,
#                 comment is None or step.comment == comment,
#                 status is None or step.status == status,
#             )
#         )
#
#     selection = [step for step in steps.values() if check_step(step)]
#     return{
#         "query": {"script": script, "comment": comment},
#         "selection": selection
#     }

# TODO status is a protected field, automatically set to PRELAUNCH.
@app.post("/")
def add_step(step: schemas.StepCreate,
             db: Session = Depends(get_db)
             ):
    db_step = db.query(models.Step).filter(models.Step.id == step.id).first()
    if db_step:
        HTTPException(
            status_code=400, detail=f"Step with {step.id=} already exist."
        )
    db_step = models.Step(**step.dict())
    db.add(db_step)
    db.commit()
    db.refresh(db_step)
    return {"added": db_step}

@app.delete("/step/{step_id}")
def delete_step(step_id: int, db: Session = Depends(get_db)):
    # TODO check what is step.status. Only if prelaunch should be deleted!
    db_step = db.query(models.Step).filter(models.Step.id == step_id).delete()
    db.commit()
    return {"deleted": db_step}


# TODO using REST for RPC is not the most elegant way to do it.
@app.post("/run/{step_id}")
def run_step(step_id: int, db: Session = Depends(get_db)):
    db_step = db.query(models.Step).filter(models.Step.id == step_id).first()
    if not db_step:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    if db_step.status != "prelaunch":
        raise HTTPException(
            status_code=403, detail=f"Step with {step_id=} is not ready for execution."
        )
    db_step.run_script()
    db_step.status = "running"
    db.commit()
    return {"started": db_step}


@app.post("/completed/{step_id}")
def report_completed(step_id: int, db: Session = Depends(get_db)):
    db_step = db.query(models.Step).filter(models.Step.id == step_id).first()
    if not db_step:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    if db_step.status != "running":
        raise HTTPException(
            status_code=403, detail=f"Step with {step_id=} is not running, how can it be completed?"
        )
    db_step.status = "completed"
    db.commit()
    return {"completed": db_step}
