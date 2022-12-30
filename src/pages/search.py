import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from flask import request


import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
from datetime import date
import pandas as pd
from elasticsearch.helpers import scan

import utils.helpers as hp
from model.Alarms import Alarms

import urllib3
urllib3.disable_warnings()



def title():
    return f"Search & explore"



def description(q=None):
    return f"Search & explore the Networking alarms"



dash.register_page(
    __name__,
    path_template="/search-alarms",
    title=title,
    description=description,
)
        

def convertTime(ts):
    stripped = datetime.strptime(ts, '%Y-%m-%d %H:%M')
    return int((stripped - datetime(1970, 1, 1)).total_seconds()*1000)



def layout(**other_unknown_query_strings):
    now = hp.defaultTimeRange(days=1, datesOnly=True)

    return dbc.Row([
        
        dbc.Row([
            dbc.Col(
                dcc.Loading(
                  html.Div(id="alarms-sunburst"),
                style={'height':'0.5rem'}, color='#00245A'),
            align="start", width='5', className="mr-1"),
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        html.H1(f"Search & explore the Networking alarms", className="l-h-3 pl-2"),
                    ], width=10, align="center", className="text-left pair-details rounded-border-1")
                ], justify="start", align="center"),
                html.Br(),
                html.Br(),
                html.Br(),
                dbc.Row([
                    dcc.DatePickerRange(
                        id='date-picker-range',
                        month_format='M-D-Y',
                        min_date_allowed=date(2022, 8, 1),
                        initial_visible_month=now[0],
                        start_date=now[0],
                        end_date=now[1]
                    ),
                    html.P('Rounded to the day', style={"padding-left":"1.5%"})
                ]),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='sites-dropdown', placeholder="Search for a site"),
                    ], width=10),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='events-dropdown', placeholder="Search for an event type"),
                    ], width=10),
                ]),
               
            ]),
        ], className="p-1 site boxwithshadow page-cont mb-2 g-0", justify="center", align="center"),
        html.Br(),
        html.Br(),
        dbc.Row([
            dbc.Row([
                html.H1(f"List of alarms", className="text-center"),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table'),
                style={'height':'0.5rem'}, color='#00245A')
            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-2 g-0", justify="center", align="center"),
        html.Br(),
        html.Br(),
    ], className='g-0 main-cont', align="start", style={"padding": "0.5% 1.5%"})



def colorMap(eventTypes):
  colors = ['#75cbe6', '#3b6d8f', '#75E6DA', '#189AB4', '#2E8BC0', '#145DA0', '#05445E', '#0C2D48',
          '#5EACE0', '#d6ebff', '#498bcc', '#82cbf9', 
          '#2894f8', '#fee838', '#3e6595', '#4adfe1', '#b14ae1'
          '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
          ]

  paletteDict = {}
  for i,e in enumerate(eventTypes):
      paletteDict[e] = colors[i]
  
  return paletteDict



@dash.callback(
    [
        Output("sites-dropdown", "options"),
        Output("events-dropdown", "options"),
        Output('alarms-sunburst', 'children'),
        Output('results-table', 'children'),
    ],
    [
      Input('date-picker-range', 'start_date'),
      Input('date-picker-range', 'end_date'),
      Input("sites-dropdown", "search_value"),
      Input("sites-dropdown", "value"),
      Input("events-dropdown", "search_value"),
      Input("events-dropdown", "value"),
    ],
    State("sites-dropdown", "value"),
    State("events-dropdown", "value"))
def update_output(start_date, end_date, sites, all, events, allevents, sitesState, eventsState ):

    if start_date and end_date:
        period = [f'{start_date} 00:01', f'{end_date} 23:59']
    else: period = hp.defaultTimeRange(1)

    alarmsInst = Alarms()
    frames, pivotFrames = alarmsInst.loadData(period[0], period[1])

    scntdf = pd.DataFrame()
    for e, df in pivotFrames.items():
        df = df[df['tag'] != ''].groupby('tag')[['id']].count(
        ).reset_index().rename(columns={'id': 'cnt', 'tag': 'site'})
        df['event'] = e
        scntdf = pd.concat([scntdf, df])

    # sites
    graphData = scntdf.copy()
    if (sitesState is not None and len(sitesState) > 0):
        graphData = graphData[graphData['site'].isin(sitesState)]

    sdropdown_items = []
    for s in sorted(scntdf['site'].unique()):
        sdropdown_items.append({"label": s.upper(), "value": s.upper()})

    # events
    if eventsState is not None and len(eventsState) > 0:
        graphData = graphData[graphData['event'].isin(eventsState)]

    edropdown_items = []
    for e in sorted(scntdf['event'].unique()):
        edropdown_items.append({"label": e, "value": e})


    fig = go.Figure(data=px.sunburst(
        graphData, path=['event', 'site'],
        values='cnt',
        color='event',
        color_discrete_map=colorMap(pivotFrames.keys())
        ))
    fig.update_layout(
    margin=dict(l=20, r=20, t=20, b=20),
    )

    dataTables = []
    events = list(pivotFrames.keys()) if not eventsState or events else eventsState

    for event in sorted(events):
        df = pivotFrames[event]
        df = df[df['tag'].isin(sitesState)] if sitesState is not None and len(sitesState) > 0 else df
        if len(df) > 0:
            dataTables.append(generate_tables(
                frames[event], df, event, alarmsInst))
    dataTables = html.Div(dataTables)


    return [sdropdown_items, edropdown_items, dcc.Graph(figure=fig), dataTables]
   


def list2rows(df):
    s = df.apply(lambda x: pd.Series(x['tag']),axis=1).stack().reset_index(level=1, drop=True)
    s.name = 'tag'
    df = df.drop('tag', axis=1).join(s)

    return df


def list2str(vals, sign):
    values = vals.values
    temp = ''
    for i, s in enumerate(values[0]):
        temp += f'{s}: {sign}{values[1][i]}% \n'

    return temp



# '''Takes selected site from the Geo map and generates a Dash datatable'''
def generate_tables(frame, pivotFrames, event, alarmsInst):
    ids = pivotFrames['id'].values
    dfr = frame[frame.index.isin(ids)]
    dfr = alarmsInst.formatDfValues(dfr, event).sort_values('to', ascending=False)

    if event.startswith('bandwidth'):
        page = 'throughput/'
    elif event == 'path changed':
        page = 'paths/'
    elif event in ['firewall issue', 'complete packet loss', 'bandwidth decreased from/to multiple sites', 'high packet loss on multiple links', 'high packet loss']:
        page = 'loss-delay/'

    columns = dfr.columns.tolist()
    columns.remove('tag')
    columns.remove('id')

    # create clickable cells leading to alarm pages
    if 'alarm_id' in columns:
        url = f'{request.host_url}{page}'
        dfr['alarm_id'] = dfr['alarm_id'].apply(lambda id: f"<a href='{url}{id}' target='_blank'>VIEW</a>")

    
    if event in ['bandwidth decreased from/to multiple sites',
                 'bandwidth increased from/to multiple sites',
                 'high packet loss on multiple links']:
        columns = [el for el in columns if el not in ['src_sites', 'dest_sites',
                                                      'src_change', 'dest_change',
                                                      'src_loss%', 'dest_loss%', 'tag_str']]

    element = html.Div([
                html.Br(),
                html.H3(event.upper()),
                dash_table.DataTable(
                    data=dfr[columns].to_dict('records'),
                    columns=[{"name": i, "id": i, "presentation": "markdown"} for i in columns],
                    markdown_options={"html": True},
                    id=f'search-tbl-{event}',
                    page_current=0,
                    page_size=20,
                    style_cell={
                        'padding': '2px',
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
                    filter_action="native",
                    sort_action="native",
                ),
            ], className='single-table')

    return element

    