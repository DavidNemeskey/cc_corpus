from fastapi import Depends, FastAPI, Form, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session


from . import models, schemas
from .database import SessionLocal, engine


# TODO this should be replaced by proper migrations using the Alembic library
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="CC Corpus manager")
app.mount("/static",
          StaticFiles(directory="app/static"),
          name="static")
templates = Jinja2Templates(directory="app/templates")


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
            {
                "script": "remove_boilerplate.py",
                "input": "../test_corpus2/04a_index_sorted",
                "output": "../test_corpus2/05_boilerplate_removed",
                "further_params":
                    "-w ../test_corpus2/04_downloaded -b justext",
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


@app.get("/", response_class=HTMLResponse)
def index(request: Request, db: Session = Depends(get_db)):
    steps = db.query(models.Step).all()
    context = {"request": request, "steps": steps}
    return templates.TemplateResponse("list_steps.html", context)
    # return {"steps": steps}


@app.get("/step/{step_id}", response_class=HTMLResponse)
def query_step_by_id(step_id: int, request: Request,
                     db: Session = Depends(get_db)):
    db_step = db.query(models.Step).filter(models.Step.id == step_id).first()
    if not db_step:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


@app.post("/api/create_step/", response_model=schemas.Step)
def add_step_from_json(step: schemas.StepCreate,
                       db: Session = Depends(get_db)
                       ) -> schemas.Step:
    db_step = models.Step(**step.dict())
    db_step.status = "prelaunch"
    db.add(db_step)
    db.commit()
    db.refresh(db_step)
    return db_step


@app.get("/create_step_form/", response_class=HTMLResponse)
def create_step_form(request: Request):
    context = {"request": request}
    return templates.TemplateResponse("new_step.html", context)


@app.post("/create_step/", response_class=HTMLResponse)
def add_step_from_form(request: Request,
                       db: Session = Depends(get_db),
                       stepScript: str = Form(...),
                       stepInput: str = Form(...),
                       stepOutput: str = Form(...),
                       stepFurtherParams: str = Form(...),
                       stepScriptVersion: str = Form(...),
                       stepComment: str = Form(...),
                       ):
    step = {"script": stepScript,
            "input": stepInput,
            "output": stepOutput,
            "further_params": stepFurtherParams,
            "script_version": stepScriptVersion,
            "comment": stepComment}
    db_step = models.Step(**step)
    db_step.status = "prelaunch"
    db.add(db_step)
    db.commit()
    db.refresh(db_step)
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


@app.delete("/step/{step_id}")
def delete_step(step_id: int, db: Session = Depends(get_db)):
    # TODO check what is step.status. Only if prelaunch should be deleted!
    db_step = db.query(models.Step).filter(models.Step.id == step_id).delete()
    db.commit()
    return {"deleted": db_step}


@app.post("/run/{step_id}")
def run_step(step_id: int, db: Session = Depends(get_db)):
    db_step = db.query(models.Step).filter(models.Step.id == step_id).first()
    if not db_step:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    if db_step.status != "prelaunch":
        raise HTTPException(
            status_code=403,
            detail=f"Step with {step_id=} is not ready for execution."
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
            status_code=403,
            detail=f"Step with {step_id=} is not running, "
                   f"how can it be completed?"
        )
    db_step.status = "completed"
    db.commit()
    return {"completed": db_step}
