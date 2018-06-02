#!/usr/bin/env python2
from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for
)
from werkzeug.exceptions import abort
import sqlite3

bp = Blueprint('collector', __name__)

@bp.route('/', methods=('GET', 'POST'))
def index():
    """
    Invoked when a client connects to post the results of a finished job.
    Takes the job and adds it to the database. 
    """
    return None
