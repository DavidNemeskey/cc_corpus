#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A CRUD layer for basic interactions with the DB.
"""

from sqlalchemy.orm import Session
from . import models, schemas


def get_steps(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Step).offset(skip).limit(limit).all()


def get_step_by_id(db: Session, step_id: int):
    return db.query(models.Step).filter(models.Step.id == step_id).first()


def create_step(db: Session, step: schemas.StepCreate):
    db_step = models.Step(**step.dict())
    db_step.status = "prelaunch"
    db.add(db_step)
    db.commit()
    db.refresh(db_step)
    return db_step


def delete_step_by_id(db: Session, step_id: int):
    db.query(models.Step).filter(models.Step.id == step_id).delete()
    db.commit()
