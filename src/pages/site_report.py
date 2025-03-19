from datetime import datetime
from functools import lru_cache

import numpy as np
import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import pandas as pd
from flask import request

from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet



def title(q=None):
    return f"Report for {q}"



def description(q=None):
    return f"Weekly Site Overview for {q}"



dash.register_page(
    __name__,
    path_template="/site_report/<q>",
    title=title,
    description=description,
)

def layout(q=None, **other_unknown_query_strings):
    pq = Parquet()
    alarmsInst = Alarms()
    fromDay, toDay = hp.defaultTimeRange(7)
    frames, pivotFrames = alarmsInst.loadData(fromDay, toDay)
    alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
    alarmCnt = alarmCnt[alarmCnt['site'] == q]
    print(f"fromDay: {fromDay}, toDay: {toDay}")
    print(alarmCnt)
    return html.Div()
