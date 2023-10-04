from fastapi import Depends, FastAPI, Form, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session


from . import crud, models, seed_db, schemas
from .database import SessionLocal, engine


# TODO this should be replaced by proper migrations using the Alembic library
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="CC Corpus manager")
app.mount("/static",
          StaticFiles(directory="app/static"),
          name="static")
templates = Jinja2Templates(directory="app/templates")
favicon_path = 'app/static/favicon.ico'


@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse(favicon_path)


# DB Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# TODO this is for development purposes. Will we need seeding for production?
@app.on_event("startup")
def dev_seed():
    """Seeds the DB if it is empty."""
    db = SessionLocal()
    num_steps = db.query(models.Step).count()
    if num_steps == 0:
        seed_db.seed_steps(db)
    else:
        print(f"We already have {num_steps} records in our DB")


@app.get("/", response_class=HTMLResponse)
def index(request: Request, db: Session = Depends(get_db)):
    steps = crud.get_steps(db)
    context = {"request": request, "steps": steps}
    return templates.TemplateResponse("list_steps.html", context)
    # return {"steps": steps}


@app.get("/step/{step_id}", response_class=HTMLResponse)
def query_step_by_id(step_id: int, request: Request,
                     db: Session = Depends(get_db)):
    db_step = crud.get_step_by_id(db, step_id)
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
    return crud.create_step(db, step)


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
    step = schemas.StepCreate(
        script=stepScript,
        input=stepInput,
        output=stepOutput,
        further_params=stepFurtherParams,
        script_version=stepScriptVersion,
        comment=stepComment)
    db_step = crud.create_step(db, step)
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


@app.delete("/step/{step_id}")
def delete_step(step_id: int, db: Session = Depends(get_db)):
    # TODO check what is step.status. Only if prelaunch should be deleted?
    crud.delete_step_by_id(db, step_id)


@app.post("/run/{step_id}")
def run_step(step_id: int, db: Session = Depends(get_db)):
    # TODO should this be here or in the crud.py?
    db_step = db.query(models.Step).filter(models.Step.id == step_id).first()
    if not db_step:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    # TODO commented this one out to make testing easier:
    # if db_step.status != "prelaunch":
    #     raise HTTPException(
    #         status_code=403,
    #         detail=f"Step with {step_id=} is not ready for execution."
    #     )
    db_step.run_script()
    db_step.status = "running"
    db.commit()
    return {"started": db_step}


@app.post("/completed/{step_id}")
def report_completed(step_id: int, db: Session = Depends(get_db)):
    # TODO should this be here or in the crud.py?
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
