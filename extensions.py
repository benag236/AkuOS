"""Shared Flask extension singletons.

Defining the SQLAlchemy instance here (separate from app.py and models.py)
avoids the circular import that would otherwise happen when models.py needs
`db` but app.py also needs to import models.
"""
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
