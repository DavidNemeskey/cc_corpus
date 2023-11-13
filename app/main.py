from fastapi import Depends, FastAPI, Form, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from json import loads
from sqlalchemy.orm import Session


from . import crud, models, seed_db, schemas
from .config import config
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


def get_db():
    """Supplies the DB session to methods via dependency."""
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


# This is a GET route which displays the form to create a new step
@app.get("/create_step_form/", response_class=HTMLResponse)
def create_step_form(request: Request):
    step_types = list(config["scripts"].keys())
    context = {"request": request, "step_types": step_types}
    return templates.TemplateResponse("new_step.html", context)


@app.post("/create_step/", response_class=HTMLResponse)
def add_step_from_form(request: Request,
                       db: Session = Depends(get_db),
                       stepName: str = Form(...),
                       comment: str = Form(default=None),
                       ):
    step = schemas.StepCreate(
        step_name=stepName,
        comment=comment)
    db_step = crud.create_step(db, step)
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


@app.post("/api/create_step/", response_model=schemas.Step)
def add_step_from_json(step: schemas.StepCreate,
                       db: Session = Depends(get_db)
                       ) -> schemas.Step:
    return crud.create_step(db, step)


# This is a GET route which displays the form to edit a step
@app.get("/edit_step/{step_id}", response_class=HTMLResponse)
def edit_step(step_id: int,
              request: Request,
              db: Session = Depends(get_db)
              ):
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
def update_step_from_form(request: Request,
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
def update_step(step: schemas.StepUpdate, db: Session = Depends(get_db)):
    crud.update_step(db, step)


@app.delete("/step/{step_id}")
def delete_step(step_id: int, db: Session = Depends(get_db)):
    crud.delete_step_by_id(db, step_id)


@app.post("/run/{step_id}", response_class=HTMLResponse)
def run_step(step_id: int,
             request: Request,
             db: Session = Depends(get_db)
             ):
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
    context = {"request": request, "step": db_step}
    return templates.TemplateResponse("view_step.html", context)


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


@app.get("/pipelines/", response_class=HTMLResponse)
def list_pipelines(request: Request, db: Session = Depends(get_db)):
    pipelines = crud.get_pipelines(db)
    pipeline_types = list(config["pipelines"].keys())
    context = {"request": request,
               "pipelines": pipelines,
               "pipeline_types": pipeline_types}
    return templates.TemplateResponse("list_pipelines.html", context)


@app.get("/pipeline/{pipeline_id}", response_class=HTMLResponse)
def query_pipeline_by_id(pipeline_id: int,
                         request: Request,
                         db: Session = Depends(get_db)
                         ):
    db_pipeline = crud.get_pipeline_by_id(db, pipeline_id)
    if not db_pipeline:
        raise HTTPException(
            status_code=404,
            detail=f"Pipeline with {pipeline_id=} does not exist."
        )
    context = {"request": request, "pipeline": db_pipeline}
    return templates.TemplateResponse("view_pipeline.html", context)


# This is a GET route which displays the form
# to create a new pipeline of a given type
@app.get("/create_pipeline_form/{pipeline_type}", response_class=HTMLResponse)
def create_pipeline_form(pipeline_type: str, request: Request):
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
    form_data = await request.form()
    form_data = jsonable_encoder(form_data)
    pipeline = schemas.PipelineCreate(
        template=form_data.pop("template"),
        comment=form_data.pop("comment"),
        params=form_data,
    )
    db_pipeline = crud.create_pipeline(db, pipeline)
    context = {"request": request, "pipeline": db_pipeline}
    return templates.TemplateResponse("view_pipeline.html", context)


# This is a GET route which displays the form to edit a step
@app.get("/edit_pipeline/{pipeline_id}", response_class=HTMLResponse)
def edit_pipeline(pipeline_id: int,
                  request: Request,
                  db: Session = Depends(get_db)
                  ):
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
    print("=============ROUTE===============")
    form_data = await request.form()
    form_data = jsonable_encoder(form_data)
    print(form_data)
    print(form_data["params"].__class__)
    if form_data["params"]:
        form_data["params"] = loads(form_data["params"])
    else:
        form_data["params"] = None
    if form_data["steps"]:
        form_data["steps"] = loads(form_data["steps"])
    else:
        form_data["steps"] = None
    print(form_data)
    pipeline = schemas.PipelineUpdate(**form_data)
    print(pipeline)
    crud.update_pipeline(db, pipeline)
    db_pipeline = crud.get_pipeline_by_id(db, pipeline.id)
    context = {"request": request, "pipeline": db_pipeline}
    return templates.TemplateResponse("view_pipeline.html", context)


@app.post("/spawn/{pipeline_id}", response_class=HTMLResponse)
def spawn_pipeline(pipeline_id: int,
                   request: Request,
                   db: Session = Depends(get_db)
                   ):
    db_pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
    print("=======SPAWNER==========")
    print(db_pipeline)
    step_types = config["pipelines"][db_pipeline.template]["steps"]
    print(step_types)

    config_mod = {}
    for key, value in db_pipeline.params.items():
        if key == "cc_batch":
            config_mod[key] = value
        # TODO I am not happy with this hardwiring...
        if key == "url_pattern":
            config_mod.setdefault("scripts", {}).setdefault("get_indexfiles", {})["p"] = value
    print("The config modifier:")
    print(config_mod)
    step_ids = []
    for step_type in step_types:
        step_name = step_type
        step = schemas.StepCreate(step_name=step_name,
                                  comment=f"Spawned by Pipeline {pipeline_id}")
        db_step = crud.create_step(db, step, config_mod)
        print(db_step.id)
        step_ids.append(db_step.id)
    db_pipeline.steps = jsonable_encoder(step_ids)
    print(db_pipeline.steps)
    db_pipeline.status = "spawned"
    db.commit()
    context = {"request": request, "pipeline": db_pipeline}
    return templates.TemplateResponse("view_pipeline.html", context)
