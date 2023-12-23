import numpy as np
import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State

import plotly.graph_objects as go
import plotly.express as px

import pandas as pd
from flask import request

from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet
import pycountry


@timer
def builMap(mapDf):
    # usually test and production sites are at the same location,
    # so we add some noise to the coordinates to make them visible
    mapDf['lat'] = mapDf['lat'].astype(float) + np.random.normal(scale=0.01, size=len(mapDf))
    mapDf['lon'] = mapDf['lon'].astype(float) + np.random.normal(scale=0.01, size=len(mapDf))

    color_mapping = {
    '⚪': '#6a6969',
    '🔴': '#c21515',
    '🟡': '#ffd500',
    '🟢': '#01a301'
    }

    size_mapping = {
    '⚪': 4,
    '🔴': 3,
    '🟡': 2,
    '🟢': 1
    }

    mapDf['size'] = mapDf['Status'].map(size_mapping)

    fig = px.scatter_mapbox(data_frame=mapDf, lat="lat", lon="lon",
                        color="Status",
                        color_discrete_map=color_mapping,
                        size_max=11,
                        size='size',
                        hover_name="site",
                        custom_data=['Infrastructure','Network','Other'],
                        zoom=1,
                    )

    fig.update_traces(
        hovertemplate="<br>".join([
            "<b>%{hovertext}</b>",
            "Infrastructure: %{customdata[0]}",
            "Network: %{customdata[1]}",
            "Other: %{customdata[2]}",
        ]),
        marker=dict(opacity=0.7)
    )

    fig.update_layout(
        margin=dict(t=0, b=0, l=0, r=0),
        mapbox=dict(
            accesstoken='pk.eyJ1IjoicGV0eWF2IiwiYSI6ImNraDNwb3k2MDAxNnIyeW85MTMwYTU1eWoifQ.1QQ1E5mPh3hoZjK5X5LH7Q',
            bearing=0,
            center=go.layout.mapbox.Center(
                lat=43,
                lon=-6
            ),
            pitch=0,
            style='mapbox://styles/petyav/ckh3spvk002i419mzf8m9ixzi'
        ),
        showlegend=False,
        title = 'Status of all sites in the past 24 hours',
        template='plotly_white'
    )

    return fig


@timer
def generate_status_table(alarmCnt):

    red_sites = alarmCnt[(alarmCnt['event']=='bandwidth decreased from/to multiple sites')
            & (alarmCnt['cnt']>0)]['site'].unique().tolist()

    yellow_sites = alarmCnt[(alarmCnt['event']=='path changed between sites')
                    & (alarmCnt['cnt']>0)]['site'].unique().tolist()

    grey_sites = alarmCnt[(alarmCnt['event'].isin(['firewall issue', 'source cannot reach any', 'complete packet loss']))
                    & (alarmCnt['cnt']>0)]['site'].unique().tolist()

    catdf = qrs.getSubcategories()
    catdf = pd.merge(alarmCnt, catdf, on='event', how='left')

    df = catdf.groupby(['site', 'category'])['cnt'].sum().reset_index()

    df_pivot = df.pivot(index='site', columns='category', values='cnt')
    df_pivot.reset_index(inplace=True)

    df_pivot.sort_values(by=['Network', 'Infrastructure', 'Other'], ascending=False, inplace=True)


    def give_status(site):
        if site in red_sites:
            return '🔴'

        elif site in yellow_sites:
            return '🟡'
        
        elif site in grey_sites:
            return '⚪'
        return '🟢'

    df_pivot['Status'] = df_pivot['site'].apply(give_status)

    df_pivot = df_pivot[['site', 'Status', 'Network', 'Infrastructure', 'Other']]

    url = f'{request.host_url}site'
    df_pivot['url'] = df_pivot['site'].apply(lambda name: 
                                             f"<a class='btn btn-secondary' role='button' href='{url}/{name}' target='_blank'>See latest alarms</a>" if name else '-')

    status_order = ['🔴', '🟡', '🟢', '⚪']
    df_pivot = df_pivot.sort_values(by='Status', key=lambda x: x.map({status: i for i, status in enumerate(status_order)}))

    if len(df_pivot) > 0:
        element = html.Div([
                    dash_table.DataTable(
                    df_pivot.to_dict('records'),[{"name": i.upper(), "id": i, "presentation": "markdown"} for i in df_pivot.columns],
                    filter_action="native",
                    filter_options={"case": "insensitive"},
                    sort_action="native",
                    is_focused=True,
                    markdown_options={"html": True},
                    page_size=16,
                    style_cell={
                        'padding': '2px',
                        'font-size': '1.5em',
                        'textAlign': 'center',
                    },
                    style_header={
                        'backgroundColor': 'white',
                        'fontWeight': 'bold'
                    },
                    style_data={
                        'height': 'auto',
                        'overflowX': 'auto',
                    },
                    style_table={
                        'overflowY': 'auto',
                        'overflowX': 'auto'
                    },
                    style_data_conditional=[],
                    id='status-tbl')
                ], className='status-table')
    else:
        element = html.Div(html.H3('No alarms for this site in the past day'), style={'textAlign': 'center'})

    return element, pd.merge(df_pivot, alarmCnt[['site', 'lat', 'lon']].drop_duplicates(subset='site', keep='first'), on='site', how='left')


def get_country_code(country_name):
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_2
    except LookupError:
        return ''


def total_number_of_alarms(sitesDf):
    metaDf = pq.readFile('parquet/raw/metaDf.parquet')
    sitesDf = pd.merge(sitesDf, metaDf[['lat', 'lon', 'country']], on=['lat', 'lon'], how='left').drop_duplicates()
    site_totals = sitesDf.groupby('site')[['Infrastructure', 'Network', 'Other']].sum()

    highest_site = site_totals.sum(axis=1).idxmax()
    highest_site_alarms = site_totals.sum(axis=1).max()
    
    country_totals = sitesDf.groupby('country')[['Infrastructure', 'Network', 'Other']].sum()
    highest_country = country_totals.sum(axis=1).idxmax()
    highest_country_alarms = country_totals.sum(axis=1).max()
    
    status = ['🔴', '🟡', '🟢', '⚪']
    status_count = sitesDf[['Status', 'site']].groupby('Status').count().to_dict()['site']
    for s in status:
        if s not in status_count:
            status_count[s] = 0


    html_elements = [dbc.Col([
            dbc.Row(
                dbc.Col(
                    html.H1('Status of all sites in the past 24 hours', className='status-number')
                , align="center")
            , align="center", justify='center', className='h-100'),
        ], className='status-box boxwithshadow', md=3, xs=12)]
    # add the status count to the html
    total_status = [dbc.Col(html.P('Summary', className='status-number h-100 status-text'), md=3, xs=12)]
    for s in status:
        total_status.append(
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.H4(f'{s}', className='card-title'),
                            html.P(f'{status_count[s]}', className='card-text'),
                        ]
                    ),
                    className='mb-3',
                ),
                md=2, xs=6
            )
        )

    html_elements.append(dbc.Col([
        # dbc.Row(html.H3('Overall status', className='status-title b flex'), justify="start"),
        dbc.Row(children=total_status, justify="center", align="center", className='h-100')],
        className='status-box boxwithshadow col-md-auto', md=3, xs=12))

    # # add the total number of alarms to the html
    # for k,v in sitesDf.sum(numeric_only=True).to_dict().items():
    #     html_elements.append(dbc.Col([
    #         html.H3(f'Total number of {k} alarms', className='status-title'),
    #         html.H1(f'{v}', className='status-number'),
    #     ], className='status-box boxwithshadow', md=2, xs=3))

    # add the highest number of alarms based on site name to the html
    country_code = get_country_code(sitesDf[sitesDf['site']==highest_site]['country'].values[0])
    html_elements.append(dbc.Col([
        dbc.Row([
            html.H3(f'Highest number of alarms from site', className='status-title'),
            html.H1(f' {highest_site} ({country_code}): {highest_site_alarms}', className='status-number')
        ], align="center", className='h-100'),
        ], className='status-box boxwithshadow', md=3, xs=12))

    # add the highest number of alarms based on country to the html    
    html_elements.append(dbc.Col([
        dbc.Row([
            html.H3(f'Highest number of alarms from country', className='status-title'),
            html.H1(f'{highest_country}: {highest_country_alarms}', className='status-number'),
        ], align="center", className='h-100'),
        ], className='status-box boxwithshadow', md=3, xs=12))

    return html_elements


def createTable(df, id):
    return dash_table.DataTable(
            df.to_dict('records'),
            columns=[{"name": i, "id": i, "presentation": "markdown"} for i in df.columns],
                markdown_options={"html": True},
                style_cell={
                    'padding': '2px',
                    'font-size': '1.5em',
                    'textAlign': 'center',
                    'whiteSpace': 'pre-line',
                    },
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold'
                },
                style_data={
                    'height': 'auto',
                    'overflowX': 'auto',
                },
                style_table={
                    'overflowY': 'auto',
                    'overflowX': 'auto'
                },
            id=id)


def explainStatuses():

  # Infrastructure:
  # if 'firewall issue' or 'source cannot reach any' -> red
  # otherwise yellow

  # this way we can then have network like this:
  # if 'bandwidth decreased from multiple' -> red
  # elif 'path changed' -> yellow
  # elif Infrastructure = 'red' -> grey
  # else -> green

  categoryDf = qrs.getSubcategories()

  red_infrastructure = ['firewall issue', 'source cannot reach any', 'complete packet loss']

  status = [
  {
    'status category': 'Global',
      'resulted status': '🔴',
      'considered alarm types': '\n'.join(['bandwidth decreased from multiple']),
      'trigger': 'any type has > 0 alarms'
  },
  {
    'status category': 'Global',
      'resulted status': '🟡',
      'considered alarm types': '\n'.join(['path changed']),
      'trigger': 'any type has > 0 alarms'
  },
  {
    'status category': 'Global',
      'resulted status': '⚪',
      'considered alarm types': '\n'.join(['Infrastructure']),
      'trigger': 'Infrastructure status is 🔴'
  },
  {
    'status category': 'Global',
      'resulted status': '🟢',
      'considered alarm types': '',
      'trigger': 'otherwise'
  },
  {
    'status category': 'Infrastructure',
      'considered alarm types': ',\n'.join(red_infrastructure),
      'trigger': 'any type has > 0 alarms',
      'resulted status': '🔴',
  },
  {
    'status category': 'Infrastructure',
      'considered alarm types': ',\n'.join(list(set(categoryDf[categoryDf['category']=='Infrastructure']['event'].unique()) - set(red_infrastructure))),
      'trigger': 'any type has > 0 alarms',
      'resulted status': '🟡',
  }]

  status_explaned = pd.DataFrame(status)
  categoryDf = categoryDf.pivot_table(values='event', columns='category', aggfunc=lambda x: '\n \n'.join(x))

  return createTable(status_explaned, 'status_explaned'), createTable(categoryDf, 'categoryDf')


dash.register_page(__name__, path='/')

pq = Parquet()
alarmsInst = Alarms()


def layout(**other_unknown_query_strings):
    dateFrom, dateTo = hp.defaultTimeRange(1)
    alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
    statusTable, sitesDf = generate_status_table(alarmCnt)
    print("Period:", dateFrom," - ", dateTo)
    print(f'Number of alarms: {len(alarmCnt)}')

    total_number = total_number_of_alarms(sitesDf)

    return html.Div([
            dbc.Row(
                children=total_number, className='g-0 d-flex align-items-stretch h-100', align="center", justify='center',  style={"padding": "0.5% 1.5%"}),
            dbc.Row([
                dbc.Row([
                        dbc.Col(
                            [
                                html.Div(children=statusTable, id='site-status', className='datatables-cont'),
                            ], className='page-cont sidebysite-cont'
                        ),
                        dbc.Col(dcc.Graph(figure=builMap(sitesDf), id='site-map',
                                  className='cls-site-map mb-1 page-cont sidebysite-cont'), 
                        ),
                        dbc.Col(
                            html.Div(
                                [
                                    dbc.Button(
                                        "How was the status determined?",
                                        id="how-status-collapse-button",
                                        className="mb-3",
                                        color="secondary",
                                        n_clicks=0,
                                    ),
                                    dbc.Collapse(
                                        id="how-status-collapse",
                                        className="how-status-collapse",
                                        is_open=False,
                                    ),
                                ], className="how-status-div",
                            ), lg=12, md=12,
                        ),
                    ], className='boxwithshadow page-cont mb-1 g-0 p-1', justify="center", align="center"),
                html.Div(id='page-content-noloading'),
                html.Br(),
                
            ], className='g-0', align="start", style={"padding": "0.5% 1.5%"})
            ], className='main-cont')



@dash.callback(
    [
    Output("how-status-collapse", "is_open"),
    Output("how-status-collapse", "children"),
    ],
    [Input("how-status-collapse-button", "n_clicks")],
    [State("how-status-collapse", "is_open")],
)
def toggle_collapse(n, is_open):
    catTable, statusExplainedTable = explainStatuses()
    data = dbc.Row([
                    dbc.Col(children=[
                        html.H3('Category & Alarm types', className='status-title'),
                        html.Div(statusExplainedTable, className='how-status-table')
                        ], lg=6, md=12, sm=12, className='page-cont pr-1 how-status-cont'),
                    dbc.Col(children=[
                        html.H3('Status color rules', className='status-title'),
                        html.Div(catTable, className='how-status-table')], lg=6, md=12, sm=12, className='page-cont how-status-cont')
                ], className='pt-1')

    if n:
        return not is_open, data
    return is_open, data

# # # '''Takes selected site from the Geo map and displays the relevant information'''
# @dash.callback(
#     [
#         Output('datatables', 'children'),
#         Output('selected-site', 'children'),
#         Output('site-plots-in-out', 'figure'),
#     ],
#     Input('site-map', 'clickData')
# )
# def display_output(value):
#     global eventCnt

#     if value is not None:
#         location = value['points'][0]['customdata'][0]
#     # else:
#     #     location = eventCnt[eventCnt['cnt'] == eventCnt['cnt'].max()]['site'].values[0]

#     # print(location)
    
#         return []

