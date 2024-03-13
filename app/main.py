#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The main script for running the FastAPI app. Also contains all the controllers.
"""


from fastapi import BackgroundTasks, Depends, FastAPI, Form
from fastapi import Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import logging
from json import loads
from sqlalchemy.orm import Session


from . import crud, models, seed_db, schemas
from .config import config
from .database import SessionLocal, engine
from .logging import configure_logging

# Sets up the tables in the DB according to the schemas defined by models.py
#
# If we change the schemas frequently or have various versions of our code
# deployed, it should be replaced by proper migrations using for example the
# Alembic library
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="CC Corpus manager")
app.mount("/static",
          StaticFiles(directory="app/static"),
          name="static")
templates = Jinja2Templates(directory="app/templates")
favicon_path = 'app/static/favicon.ico'


configure_logging()
logging.info("Launched manager webapp")


@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse(favicon_path)


def get_db():
    """Supplies the DB session to methods via dependency."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# This is for development purposes. If we need seeding in a production
# environment, we have to reconsider the trigger condition for seeding.
@app.on_event("startup")
def dev_seed():
    """Seeds the DB if there are no step records in it."""
    db = SessionLocal()
    num_steps = db.query(models.Step).count()
    if num_steps == 0:
        seed_db.seed_steps(db)
    else:
        logging.info(f"There are {num_steps} step records in our DB")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request,
                status: str = Query(None),
                db: Session = Depends(get_db)):
    """Controller for rendering the list of steps."""
    if status:
        filter = {'status': status}
        steps = crud.get_steps(db, filters=filter)
    else:
        steps = crud.get_steps(db)
    context = {"request": request,
               "status_options": models.STEP_STATUSES,
               "selected_status": status,
               "steps": steps}
    return templates.TemplateResponse("list_steps.html", context)
    # return {"steps": steps}


@app.get("/step/{step_id}", response_class=HTMLResponse)
async def query_step_by_id(step_id: int,
                           request: Request,
                           db: Session = Depends(get_db)
                           ):
    """Controller for rendering a single step."""
    db_step = crud.get_step_by_id(db, step_id)
    if not db_step:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


@app.get("/create_step_form/", response_class=HTMLResponse)
async def create_step_form(request: Request):
    """Controller to render the create new step form."""
    step_types = list(config["scripts"].keys())
    context = {"request": request, "step_types": step_types}
    return templates.TemplateResponse("new_step.html", context)


@app.post("/create_step/", response_class=HTMLResponse)
async def add_step_from_form(request: Request,
                             db: Session = Depends(get_db),
                             stepName: str = Form(...),
                             comment: str = Form(default=None),
                             ):
    """Controller to process the response from create new step form."""
    step = schemas.StepCreate(
        step_name=stepName,
        comment=comment)
    db_step = crud.create_step(db, step)
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


@app.post("/api/create_step/", response_model=schemas.Step)
async def add_step_from_json(step: schemas.StepCreate,
                             db: Session = Depends(get_db)
                             ) -> schemas.Step:
    """Controller to process creating a new step from an API call."""
    return crud.create_step(db, step)


@app.get("/edit_step/{step_id}", response_class=HTMLResponse)
async def edit_step(step_id: int,
                    request: Request,
                    db: Session = Depends(get_db)
                    ):
    """Controller to render the edit step form."""
    db_step = crud.get_step_by_id(db, step_id)
    if not db_step:
        raise HTTPException(
            status_code=404, detail=f"Step with {step_id=} does not exist."
        )
    context = {"request": request,
               "step": db_step,
               "status_options": models.STEP_STATUSES
               }
    return templates.TemplateResponse("edit_step.html", context)


@app.post("/update_step/", response_class=HTMLResponse)
async def update_step_from_form(request: Request,
                                db: Session = Depends(get_db),
                                stepId: int = Form(...),
                                stepName: str = Form(...),
                                scriptFile: str = Form(...),
                                input: str = Form(default=None),
                                output: str = Form(...),
                                furtherParams: str = Form(default=""),
                                scriptVersion: str = Form(...),
                                comment: str = Form(default=None),
                                status: str = Form(...),
                                ):
    """Controller to process the response from the edit step form."""
    step = schemas.StepUpdate(
        id=stepId,
        step_name=stepName,
        script_file=scriptFile,
        input=input,
        output=output,
        further_params=furtherParams,
        script_version=scriptVersion,
        comment=comment,
        status=status,
    )
    crud.update_step(db, step)
    db_step = crud.get_step_by_id(db, step.id)
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


@app.put("/step")
async def update_step(step: schemas.StepUpdate,
                      db: Session = Depends(get_db)
                      ):
    """Controller to process updating a step from an API call."""
    crud.update_step(db, step)


@app.delete("/step/{step_id}")
async def delete_step(step_id: int, db: Session = Depends(get_db)):
    """Controller to delete a step from an API call."""
    crud.delete_step_by_id(db, step_id)


@app.post("/run/{step_id}", response_class=HTMLResponse)
async def run_step(step_id: int,
                   request: Request,
                   db: Session = Depends(get_db)
                   ):
    """
    Controller to execute a step.
    The step must be in the "prelaunch" status.
    Status will be changed to "running"
    """
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
    db_step.run_script(request.base_url)
    # TODO should this DB update be done here or in the run_script method?
    db_step.status = "running"
    db.commit()
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


@app.post("/completed/{step_id}")
async def report_completed(step_id: int, db: Session = Depends(get_db)):
    """
    Controller for API call to mark the completion of the execution of a step.
    It changes its status from "running" to "completed".
    """
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
    # TODO should this be here or in a crud.py method?
    db_step.status = "completed"
    db.commit()
    return {"completed": db_step}


@app.post("/failed/{step_id}")
async def report_failure(step_id: int, db: Session = Depends(get_db)):
    """
    Controller for API call to signal the failure of the execution of a step.
    It changes its status from "running" to "failed".
    """

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
                   f"how can it fail?"
        )
    db_step.status = "failed"
    db.commit()
    return {"completed": db_step}


@app.get("/pipelines/", response_class=HTMLResponse)
async def list_pipelines(request: Request,
                         status: str = Query(None),
                         db: Session = Depends(get_db)):
    """Controller for rendering the list of pipelines."""
    pipelines = crud.get_pipelines(db, status=status)
    pipeline_types = list(config["pipelines"].keys())
    context = {"request": request,
               "status_options": models.PIPELINE_STATUSES,
               "selected_status": status,
               "pipelines": pipelines,
               "pipeline_types": pipeline_types}
    return templates.TemplateResponse("list_pipelines.html", context)


@app.get("/pipeline/{pipeline_id}", response_class=HTMLResponse)
async def query_pipeline_by_id(pipeline_id: int,
                               request: Request,
                               db: Session = Depends(get_db)
                               ):
    """Controller for rendering a single pipeline."""
    db_pipeline = crud.get_pipeline_by_id(db, pipeline_id)
    if not db_pipeline:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline with {pipeline_id=} does not exist."
        )
    context = {"request": request, "pipeline": db_pipeline}
    return templates.TemplateResponse("view_pipeline.html", context)


@app.get("/create_pipeline_form/{pipeline_type}", response_class=HTMLResponse)
async def create_pipeline_form(pipeline_type: str, request: Request):
    """Controller to render the create new pipeline form of a given type."""
    params = config["pipelines"][pipeline_type]["params"]
    context = {"request": request,
               "params": params,
               "pipeline_type": pipeline_type
               }
    return templates.TemplateResponse("new_pipeline.html", context)


@app.post("/create_pipeline/", response_class=HTMLResponse)
async def add_pipeline_from_form(request: Request,
                                 db: Session = Depends(get_db),
                                 ):
    """Controller to process the response from create new pipeline form."""
    form_data = await request.form()
    form_data = jsonable_encoder(form_data)
    prereq_pipe = form_data.pop("prereq_pipe")
    prereq_step = form_data.pop("prereq_step")
    pipeline = schemas.PipelineCreate(
        template=form_data.pop("template"),
        comment=form_data.pop("comment"),
        prereq_pipe=int(prereq_pipe) if prereq_pipe != "" else None,
        prereq_step=int(prereq_step) if prereq_step != "" else None,
        params=form_data,
    )
    db_pipeline = crud.create_pipeline(db, pipeline)
    context = {"request": request, "pipeline": db_pipeline}
    return templates.TemplateResponse("view_pipeline.html", context)


@app.get("/edit_pipeline/{pipeline_id}", response_class=HTMLResponse)
async def edit_pipeline(pipeline_id: int,
                        request: Request,
                        db: Session = Depends(get_db)
                        ):
    """Controller to render the edit pipeline form."""
    db_pipeline = crud.get_pipeline_by_id(db, pipeline_id)
    if not db_pipeline:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline with {pipeline_id=} does not exist."
        )
    context = {"request": request,
               "pipeline": db_pipeline,
               "status_options": models.PIPELINE_STATUSES,
               "templates": list(config["pipelines"].keys()),
               }
    return templates.TemplateResponse("edit_pipeline.html", context)


@app.post("/update_pipeline/", response_class=HTMLResponse)
async def update_pipeline_from_form(request: Request,
                                    db: Session = Depends(get_db),
                                    ):
    """Controller to process the response from the edit pipeline form."""
    form_data = await request.form()
    form_data = jsonable_encoder(form_data)

    # Convert nested Dicts and replace empty strings with Nones:
    # This is because fastapi doesn't handle html responses elegantly.
    # If the user doesn't change a value on the frontend, we got empty strings
    # But SQL Alchemy needs Nones, otherwise it overwrites the values.
    if form_data["params"]:
        form_data["params"] = loads(form_data["params"])
    else:
        form_data["params"] = None
    if form_data["steps"]:
        form_data["steps"] = loads(form_data["steps"])
    else:
        form_data["steps"] = None
    if not form_data["prereq_pipe"]:
        form_data["prereq_pipe"] = None
    if not form_data["prereq_step"]:
        form_data["prereq_step"] = None

    pipeline = schemas.PipelineUpdate(**form_data)
    crud.update_pipeline(db, pipeline)
    db_pipeline = crud.get_pipeline_by_id(db, pipeline.id)
    context = {"request": request, "pipeline": db_pipeline}
    return templates.TemplateResponse("view_pipeline.html", context)


@app.post("/spawn/{pipeline_id}", response_class=HTMLResponse)
async def spawn_pipeline(pipeline_id: int,
                         request: Request,
                         db: Session = Depends(get_db)
                         ):
    """Controller for spawning the steps corresponding to a pipeline."""
    db_pipeline = crud.spawn_pipeline(db, pipeline_id)
    context = {"request": request, "pipeline": db_pipeline}
    return templates.TemplateResponse("view_pipeline.html", context)


@app.post("/autorun/", response_class=HTMLResponse)
async def autorun(request: Request,
                  background_tasks: BackgroundTasks,
                  db: Session = Depends(get_db),
                  ):
    """Controller to start the autorunner functionality."""
    background_tasks.add_task(crud.autorun_pipelines, db, request.base_url)
    return RedirectResponse(url='/', status_code=status.HTTP_303_SEE_OTHER)
