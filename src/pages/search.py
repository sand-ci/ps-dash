import dash
from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State

import plotly.express as px

from datetime import date
import pandas as pd


import utils.helpers as hp
from model.Alarms import Alarms

import urllib3
urllib3.disable_warnings()



def title():
    return f"Search & explore"



def description(q=None):
    return f"Search & Explore the Networking Alarms"



dash.register_page(
    __name__,
    path_template="/search-alarms",
    title=title,
    description=description,
)


def layout(**other_unknown_query_strings):
    now = hp.defaultTimeRange(days=2, datesOnly=True)

    return dbc.Row([
        
        dbc.Row([
            dbc.Col(
                dcc.Loading(
                  html.Div(id="alarms-sunburst-tab"),
                style={'height':'0.5rem'}, color='#00245A'),
            align="start", lg=7, md=12, className="pl-1 pr-1"),
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        html.H1(f"Search & Explore the Networking Alarms", className="l-h-3 pl-2"),
                    ], align="center", className="text-left pair-details rounded-border-1")
                ], justify="start", align="center"),
                html.Br(),
                html.Br(),
                html.Br(),
                dbc.Row([
                    dcc.DatePickerRange(
                        id='date-picker-range-tab',
                        month_format='M-D-Y',
                        min_date_allowed=date.today() - pd.Timedelta(days=30),
                        initial_visible_month=now[0],
                        start_date=now[0],
                        end_date=now[1]
                    ),
                    html.P('Rounded to the day', style={"padding-left":"1.5%"})
                ]),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='sites-dropdown-tab', placeholder="Search for a site"),
                    ]),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='events-dropdown-tab', placeholder="Search for an event type"),
                    ]),
                ]),
               
            ], lg=5, md=12, className="pl-1 pr-1 p-1"),
        ], className="p-1 site boxwithshadow page-cont mb-1 g-0", justify="center", align="center"),
        html.Br(),
        html.Br(),
        dbc.Row([
            dbc.Row([
                html.H1(f"List of alarms", className="text-center"),
                html.Hr(className="my-2"),
                html.Br(),
                dcc.Loading(
                    html.Div(id='results-table-tab'),
                style={'height':'0.5rem'}, color='#00245A')
            ], className="m-2"),
        ], className="p-2 site boxwithshadow page-cont mb-1 g-0", justify="center", align="center"),
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
        Output("sites-dropdown-tab", "options"),
        Output("events-dropdown-tab", "options"),
        Output('alarms-sunburst-tab', 'children'),
        Output('results-table-tab', 'children'),
    ],
    [
      Input('date-picker-range-tab', 'start_date'),
      Input('date-picker-range-tab', 'end_date'),
      Input("sites-dropdown-tab", "search_value"),
      Input("sites-dropdown-tab", "value"),
      Input("events-dropdown-tab", "search_value"),
      Input("events-dropdown-tab", "value"),
    ],
    State("sites-dropdown-tab", "value"),
    State("events-dropdown-tab", "value"))
def update_output(start_date, end_date, sites, all, events, allevents, sitesState, eventsState ):

    if start_date and end_date:
        start_date, end_date = [f'{start_date}T00:01:00.000Z', f'{end_date}T23:59:59.000Z']
    else: start_date, end_date = hp.defaultTimeRange(2)
    alarmsInst = Alarms()
    frames, pivotFrames = alarmsInst.loadData(start_date, end_date)

    # Drop that event because it is dominating the chart and "path changed" is a sumarized version of it
    event_to_drop = 'path changed between sites'
    if event_to_drop in frames:
        del frames[event_to_drop]
    if event_to_drop in pivotFrames:
        del pivotFrames[event_to_drop]

    scntdf = pd.DataFrame()
    for e, df in pivotFrames.items():
        if len(df) > 0:
            if e != 'unresolvable host': # the tag is hostname for unresolvable hosts
                df = df[df['tag'] != ''].groupby('tag')[['id']].count().reset_index().rename(columns={'id': 'cnt', 'tag': 'site'})
            else: df = df[df['site'] != ''].groupby('site')[['id']].count().reset_index().rename(columns={'id': 'cnt'})
            
            df['event'] = e
            scntdf = pd.concat([scntdf, df])

    # sites
    graphData = scntdf.copy()
    if (sitesState is not None and len(sitesState) > 0):
        graphData = graphData[graphData['site'].isin(sitesState)]

    sites_dropdown_items = []
    for s in sorted(scntdf['site'].unique()):
        if s:
            sites_dropdown_items.append({"label": s.upper(), "value": s.upper()})

    # events
    if eventsState is not None and len(eventsState) > 0:
        graphData = graphData[graphData['event'].isin(eventsState)]

    events_dropdown_items = []
    for e in sorted(scntdf['event'].unique()):
        events_dropdown_items.append({"label": e, "value": e})


    fig = px.treemap(graphData, path=[px.Constant("All alarms"), 'event', 'site'], values='cnt',
                     color='cnt',
                     color_continuous_scale=px.colors.sequential.RdBu_r,
                     )
    fig.update_layout(margin=dict(l=20, r=20, t=20, b=20))

    dataTables = []
    events = list(pivotFrames.keys()) if not eventsState or events else eventsState

    for event in sorted(events):
        df = pivotFrames[event]
        df = df[df['site'].isin(sitesState)] if sitesState is not None and len(sitesState) > 0 else df
        if len(df) > 0:
            dataTables.append(generate_tables(frames[event], df, event, alarmsInst))
    dataTables = html.Div(dataTables)


    return [sites_dropdown_items, events_dropdown_items, dcc.Graph(figure=fig), dataTables]



# '''Takes selected site from the Geo map and generates a Dash datatable'''
def generate_tables(frame, unpacked, event, alarmsInst):
    ids = unpacked['id'].values
    dfr = frame[frame.index.isin(ids)]
    dfr = alarmsInst.formatDfValues(dfr, event)
    dfr.sort_values('to', ascending=False, inplace=True)

    try:
        element = html.Div([
                    html.Br(),
                    html.H3(event.upper()),
                    dash_table.DataTable(
                        data=dfr.to_dict('records'),
                        columns=[{"name": i, "id": i, "presentation": "markdown"} for i in dfr.columns],
                        markdown_options={"html": True},
                        id=f'search-tbl-{event}-tab',
                        page_current=0,
                        page_size=10,
                        style_cell={
                            'padding': '2px',
                            'font-size': '13px',
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
                        style_table={
                        'overflowY': 'auto',
                        'overflowX': 'auto'
                        },
                        filter_action="native",
                        filter_options={"case": "insensitive"},
                        sort_action="native",
                    ),
                ], className='single-table')
        return element
    except Exception as e:
        print('dash_table.DataTable expects each cell to contain a string, number, or boolean value', e)
        return html.Div()

