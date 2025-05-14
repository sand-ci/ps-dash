from datetime import datetime

import numpy as np
import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from utils.components import siteMeasurements

import pandas as pd
from flask import request

from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet



def title(q=None):
    return f"Alarms for {q}"



def description(q=None):
    return f"List of latest alarms for site {q}"



dash.register_page(
    __name__,
    path_template="/site/<q>",
    title=title,
    description=description,
)


@timer
# '''Takes selected site from the Geo map and generates a Dash datatable'''
def generate_tables(site, dateFrom, dateTo, frames, pivotFrames, alarms4Site, alarmsInst):
    
    catdf = qrs.getSubcategories()
    catdf = catdf.groupby('category')['event'].apply(list)

    # reorder the categories
    catdf = pd.concat([catdf.loc[['Network']], catdf.loc[['Infrastructure']], catdf.loc[~catdf.index.isin(['Network', 'Infrastructure'])]]).reset_index()
    
    if site:
        out = []

        if alarms4Site['cnt'].sum() > 0:
            for cat, events in catdf.values:
                category_list = []

                for event in sorted(events):

                    if event in pivotFrames.keys():
                        eventDf = pivotFrames[event]
                        # find all cases where selected site was pinned in tag field
                        ids = eventDf[(eventDf['tag'] == site) & ((eventDf['to'] >= dateFrom) & (eventDf['to'] <= dateTo))]['id'].values

                        tagsDf = frames[event]
                        dfr = tagsDf[tagsDf.index.isin(ids)]
                        
                        if len(dfr)>0:
                            dfr = alarmsInst.formatDfValues(dfr, event).sort_values('to', ascending=False)

                            if len(ids):
                                element = html.Div([
                                    dbc.Row([
                                        dbc.Col(html.H2(event.upper()), lg=8, md=6, align="center"),
                                    ], className='mb-1', justify="between"),
                                    dbc.Row(
                                        dash_table.DataTable(
                                            data=dfr.to_dict('records'),
                                            columns=[{"name": i, "id": i, "presentation": "markdown"} for i in dfr.columns],
                                            markdown_options={"html": True},
                                            id='tbl',
                                            page_size=20,
                                            style_cell={
                                                'padding': '2px',
                                                'font-size': '14px',
                                                'whiteSpace': 'pre-line'
                                            },
                                            style_header={
                                                'backgroundColor': 'white',
                                                'fontWeight': 'bold'
                                            },
                                            style_data={
                                                'height': 'auto',
                                                'lineHeight': '15px',
                                                'overflowX': 'auto'
                                            },
                                            style_table={'overflowY': 'auto', 'overflowX': 'auto'},
                                            filter_action="native",
                                            sort_action="native",
                                        ),
                                    )
                                ], className='single-table mb-1')

                                category_list.append(element)
                    
                
                if len(category_list) == 0:
                    category_list.append(html.H2(f'No alarms in that category'))

                out.append(
                    dbc.Row(
                        dbc.Card([
                            dbc.CardHeader(html.H4(f'{cat.upper()} ALARMS', className="card-title")),
                            dbc.CardBody([
                                html.Div(category_list)
                            ], className="text-dark p-1"),
                        ], className="mb-4 site-alarms-tables boxwithshadow"
                    ), className="mb-1 g-0 align-items-start"))
                    
                    
        else:
            out = dbc.Row(
                    html.H3('No alarms for this site in the past day', className="mb-4 site-alarms-tables")
                )

    return out






def layout(q=None, **other_unknown_query_strings):
  pq = Parquet()
  alarmsInst = Alarms()
  # that period should match on /home page
  dateFrom, dateTo = hp.defaultTimeRange(2)
  frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)

  alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
  alarmCnt = alarmCnt[alarmCnt['site'] == q]

  if q:
    print('URL query:', q)
    print(f'Number of alarms: {len(alarmCnt)}')
    print()
  
    return html.Div(
            dbc.Row([
                dbc.Row([
                    dbc.Col(
                        children=html.H1(f'{q}'), 
                    id='selected-site', className='cls-selected-site', align="center"),
                    dbc.Col([
                        html.H2(f'Alarms reported in the past 24 hours', className='pb-1'),         
                        dbc.Button('Download as image', id='download-image', className='btn btn-secondary load-pairs-button dowload-page-button'),
                    ], className='cls-selected-site w-100')
                ], align="center", justify="center", className='boxwithshadow mb-1 g-0'),

                html.Div(id='datatables',
                            children=generate_tables(q, dateFrom, dateTo, frames, pivotFrames, alarmCnt, alarmsInst),
                            ),
                html.Div(id='measurements',
                            children=siteMeasurements(q, pq),
                            ),
                html.Br(),
                
            ], className='g-0', align="start", style={"padding": "0.5% 1.5%"}), className='main-cont')
