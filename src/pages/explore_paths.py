import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from flask import request

import plotly.graph_objects as go
import plotly.express as px

from datetime import date
import pandas as pd

import utils.helpers as hp
from model.Alarms import Alarms



def title():
    return f"Search & explore"



def description(q=None):
    return f"Explore the 'Path changed' alarms"



dash.register_page(
    __name__,
    path_template="/explore-paths",
    title=title,
    description=description,
)


def layout(**other_unknown_query_strings):

    now = hp.defaultTimeRange(days=7, datesOnly=True)

    return dbc.Row([

        dbc.Row([
            dbc.Col(
                dcc.Loading(
                    html.Div(id="paths-alarms-sunburst"),
                    style={'height': '0.5rem'}, color='#00245A'),
                align="start", width='5', className="mr-1"),
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        html.H1(f"Search the \"Path changed\" alarms",
                                className="l-h-3 pl-2"),
                    ], width=10, align="center", className="text-left pair-details rounded-border-1")
                ], justify="start", align="center"),
                html.Br(),
                html.Br(),
                html.Br(),
                dbc.Row([
                    dcc.DatePickerRange(
                        id='paths-date-picker-range',
                        month_format='M-D-Y',
                        min_date_allowed=date(2022, 8, 1),
                        initial_visible_month=now[0],
                        start_date=now[0],
                        end_date=now[1]
                    ),
                    html.P('Rounded to the day', style={
                           "padding-left": "1.5%"})
                ]),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='paths-sites-dropdown',
                                     placeholder="Search for a site"),
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
                    html.Div(id='paths-results-table'),
                    style={'height': '0.5rem'}, color='#00245A')
            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-2 g-0", justify="center", align="center"),
        html.Br(),
        html.Br(),
    ], className='g-0 main-cont', align="start", style={"padding": "0.5% 1.5%"})


def colorMap(eventTypes):
  colors = ['#75cbe6', '#3b6d8f', '#75E6DA', '#189AB4', '#2E8BC0', '#145DA0', '#05445E', '#0C2D48',
            '#5EACE0', '#d6ebff', '#498bcc', '#82cbf9',
            '#2894f8', '#fee838', '#3e6595', '#4adfe1', '#b14ae1'
            '#1f77b4', '#ff7f0e', '#2ca02c', '#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            ]

  paletteDict = {}
  for i, e in enumerate(eventTypes):
      paletteDict[e] = colors[i]

  return paletteDict


@dash.callback(
    [
        Output("paths-sites-dropdown", "options"),
        Output('paths-alarms-sunburst', 'children'),
        Output('paths-results-table', 'children'),
    ],
    [
        Input('paths-date-picker-range', 'start_date'),
        Input('paths-date-picker-range', 'end_date'),
        Input("paths-sites-dropdown", "search_value"),
        Input("paths-sites-dropdown", "value"),
    ],
    State("paths-sites-dropdown", "value"))
def update_output(start_date, end_date, sites, sitesState, eventsState):

    if start_date and end_date:
        period = [f'{start_date} 00:01', f'{end_date} 23:59']
    else:
        period = hp.defaultTimeRange(1)

    alarmsInst = Alarms()
    frames, pivotFrames = alarmsInst.loadData(period[0], period[1])
    frames = {'path changed between sites': frames['path changed between sites'],
              'path changed': frames['path changed']}
    pivotFrames = {'path changed between sites': pivotFrames['path changed between sites'],
                   'path changed': pivotFrames['path changed'], }


    df =  pivotFrames['path changed between sites']
    scntdf = df[df['tag'] != ''].groupby('tag')[['id']].count().reset_index().rename(columns={'id': 'cnt', 'tag': 'site'})

    # sites
    graphData = scntdf.copy()
    if (sitesState is not None and len(sitesState) > 0):
        graphData = graphData[graphData['site'].isin(sitesState)]

    sdropdown_items = []
    for s in sorted(scntdf['site'].unique()):
        sdropdown_items.append({"label": s.upper(), "value": s.upper()})

    fig = go.Figure(data=px.sunburst(
        graphData, path=['site'],
        values='cnt',
        # color='site',
        # color_discrete_map=colorMap(sites)
    ))
    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
    )

    dataTables = []

    for event in sorted(['path changed','path changed between sites']):
        df = pivotFrames[event]
        df = df[df['tag'].isin(sitesState)] if sitesState is not None and len(
            sitesState) > 0 else df
        if len(df) > 0:
            dataTables.append(generate_tables(
                frames[event], df, event, alarmsInst))
    dataTables = html.Div(dataTables)


    return [sdropdown_items, dcc.Graph(figure=fig), dataTables]


def list2rows(df):
    s = df.apply(lambda x: pd.Series(x['tag']), axis=1).stack(
    ).reset_index(level=1, drop=True)
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
    dfr = alarmsInst.formatDfValues(
        dfr, event).sort_values('to', ascending=False)

    columns = dfr.columns.tolist()
    columns.remove('tag')
    columns.remove('id')
    if event in ['bandwidth decreased from/to multiple sites',
                 'bandwidth increased from/to multiple sites',
                 'high packet loss on multiple links']:
        columns = [el for el in columns if el not in ['src_sites', 'dest_sites',
                                                      'src_change', 'dest_change', 'src_loss%', 'dest_loss%', 'tag_str']]

    # create clickable cells leading to alarm pages
    if 'alarm_id' in columns:
        page = 'paths/' if event == 'path changed' else 'throughput/'
        url = f'{request.host_url}{page}'
        dfr['alarm_id'] = dfr['alarm_id'].apply(
            lambda id: f"<a href='{url}{id}' target='_blank'>VIEW</a>")

    element = html.Div([
        html.Br(),
        html.H3(event.upper()),
        dash_table.DataTable(
            data=dfr[columns].to_dict('records'),
            columns=[{"name": i, "id": i, "presentation": "markdown"}
                     for i in columns],
            markdown_options={"html": True},
            id=f'paths-search-tbl-{event}',
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