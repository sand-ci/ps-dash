import numpy as np
import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px

import pandas as pd
from flask import request

from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet


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
        margin={'l': 10, 'b': 0, 'r': 5, 't': 50},
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
        title = 'Status of all sites in the past 24 hours'
    )

    fig.update_layout(template='plotly_white')
    return fig


@timer
def generate_status_table(alarmCnt):
    catdf = qrs.getSubcategories()
    catdf = pd.merge(alarmCnt, catdf, on='event', how='left')

    df = catdf.groupby(['site', 'category'])['cnt'].sum().reset_index()

    df_pivot = df.pivot(index='site', columns='category', values='cnt')
    df_pivot.reset_index(inplace=True)

    df_pivot.sort_values(by=['Network', 'Infrastructure', 'Other'], ascending=False, inplace=True)

    def give_status(row):
        if row['Network'] > 0:

            if row['Infrastructure'] > 0:
                return '⚪'
            return '🔴'

        elif row['Infrastructure'] == 0 and row['Network'] == 0:
            return '🟢'
        
        else: return '🟡'

    df_pivot['Status'] = df_pivot.apply(give_status, axis=1)
    df_pivot = df_pivot[['site', 'Status', 'Network', 'Infrastructure', 'Other']]

    url = f'{request.host_url}site'
    df_pivot['url'] = df_pivot['site'].apply(lambda name: 
                                             f"<a class='btn btn-secondary' role='button' href='{url}/{name}' target='_blank'>See latest alarms</a>" if name else '-')

    if len(df_pivot) > 0:
        element = html.Div([
                    dash_table.DataTable(
                    df_pivot.to_dict('records'),[{"name": i.upper(), "id": i, "presentation": "markdown"} for i in df_pivot.columns],
                    filter_action="native",
                    sort_action="native",
                    is_focused=True,
                    markdown_options={"html": True},
                    page_size=16,
                    style_cell={
                        'padding': '2px',
                        'font-size': '15px',
                        'textAlign': 'center'
                    },
                    style_header={
                        'backgroundColor': 'white',
                        'fontWeight': 'bold'
                    },
                    style_data={
                        'height': 'auto',
                        'overflowX': 'auto',
                        # 'whiteSpace': 'normal',
                    },
                    style_table={'overflowY': 'auto', 'overflowX': 'auto'},
                    style_data_conditional=[],
                    id='status-tbl')
                ], className='status-table')

    else:
        element = html.Div(html.H3('No alarms for this site in the past day'), style={'textAlign': 'center'})

    return element, pd.merge(df_pivot, alarmCnt[['site', 'lat', 'lon']].drop_duplicates(subset='site', keep='first'), on='site', how='left')


dash.register_page(__name__, path='/')

pq = Parquet()
alarmsInst = Alarms()


def layout(**other_unknown_query_strings):
    dateFrom, dateTo = hp.defaultTimeRange(1)
    alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
    statusTable, sitesDf = generate_status_table(alarmCnt)
    print("Period:", dateFrom," - ", dateTo)
    print(f'Number of alarms: {len(alarmCnt)}')

    return html.Div(

            dbc.Row([
                dbc.Row([
                        dbc.Col(
                            [
                                html.Div(children=statusTable, id='site-status', className='datatables-cont'),
                            ],  md=5, xs=12, className='page-cont pl-1'
                        ),
                        dbc.Col(dcc.Graph(figure=builMap(sitesDf), id='site-map',
                                  className='cls-site-map  page-cont'),  md=7, xs=12
                        ),
                    ], className='boxwithshadow page-cont mb-1 g-0 p-1', justify="center", align="center"),
                html.Div(id='page-content-noloading'),
                html.Br(),
                
            ], className='g-0', align="start", style={"padding": "0.5% 1.5%"}), className='main-cont')



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

