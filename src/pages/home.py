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

from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
import model.queries as qrs
from utils.parquet import Parquet


def countDistEvents(alarmCnt):
    # NOTE: taking only the first found location for a given site name.
    # Alarms are mostly based on sites, and we cannot destinguish
    # at which location the issue appeared,
    # since the alrm does not (generally) contain hosts/ips
    def groupByEvent(group):
        # keep also sites with no alarms to show them on the map
        if group['cnt'].sum() == 0:
            events = ['no alarms']
            cnt = 0
        else:
            idx = [i for i, x in enumerate(group['cnt'].values) if x > 0]
            # get here only the events where cnt > 0
            events = group['event'].values[idx].tolist()
            cnt = len(events)
        return pd.Series([events, group.lat.values[0], group.lon.values[0], cnt])
    
    mapDf = alarmCnt[['site', 'event', 'lat', 'lon', 'cnt']].groupby('site').apply(groupByEvent).reset_index()
    mapDf.columns = ['site', 'event', 'lat', 'lon', 'cnt']
    mapDf['lat'] = mapDf['lat'].astype(float)
    mapDf['lon'] = mapDf['lon'].astype(float)

    return mapDf


@timer
def builMap(mapDf):
    # usually test and production sites are at the same location
    mapDf['lat'] = mapDf['lat'] + np.random.normal(scale=0.01, size=len(mapDf))
    mapDf['lon'] = mapDf['lon'] + np.random.normal(scale=0.01, size=len(mapDf))
    mapDf['cnt_size'] = mapDf['cnt'] + 1
    fig = px.scatter_mapbox(data_frame=mapDf, lat="lat", lon="lon",
                            color="cnt", 
                            size="cnt_size",
                            color_continuous_scale=px.colors.sequential.Bluered,
                            size_max=11,
                            hover_name="site",
                            custom_data=['site', 'event'],
                            zoom=1,
                        )
    fig.update_traces(
        hovertemplate="<br>".join([
            "Site: <b>%{customdata[0]}</b>",
            "Events: %{customdata[1]}",
        ]),
        marker=dict(opacity=0.8)
    )

    fig.update_layout(
        margin={'l': 10, 'b': 0, 'r': 5, 't': 50},
        mapbox=dict(
            accesstoken='pk.eyJ1IjoicGV0eWF2IiwiYSI6ImNraDNwb3k2MDAxNnIyeW85MTMwYTU1eWoifQ.1QQ1E5mPh3hoZjK5X5LH7Q',
            bearing=0,
            center=go.layout.mapbox.Center(
                lat=50,
                lon=-6
            ),
            pitch=0,
            style='mapbox://styles/petyavas/ckhyzstu92b4c19r92w0o8ayt'
        ),
        coloraxis_colorbar=dict(
            title="# events",
            thicknessmode="pixels",
            thickness=30,
            len=0.95,
        )
    )

    fig.update_layout(template='plotly_white')
    return fig


@lru_cache(maxsize=None)
def getMetaData():
    metaDf = qrs.getMetaData()
    return metaDf

@lru_cache(maxsize=None)
def loadAllTests():
    measures = pq.readFile('parquet/raw/measures.parquet')
    return measures


@timer
# Generates the 4 plot for the 
def SitesOverviewPlots(site_name):
    metaDf = getMetaData()
    alltests = loadAllTests()

    units = {
    'ps_packetloss': 'packet loss',
    'ps_throughput': 'MBps',
    'ps_owd': 'ms'
    }

    colors = ['#720026', '#e4ac05', '#00bcd4', '#1768AC', '#ffa822', '#134e6f', '#ff6150', '#1ac0c6', '#492b7c', '#9467bd',
            '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            '#c3b369', '#e1cc55', '#fee838', '#3e6595', '#4adfe1', '#b14ae1',
            '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            '#720026', '#e4ac05', '#00bcd4', '#1768AC', '#ffa822', '#134e6f', '#ff6150', '#1ac0c6', '#492b7c', '#9467bd',
            '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            '#c3b369', '#e1cc55', '#fee838', '#3e6595', '#4adfe1', '#b14ae1',]


    fig = go.Figure()
    fig = make_subplots(rows=3, cols=2, subplot_titles=("Throughput as source", "Throughput as destination",
                                                        "Packet loss as source", "Packet loss as destination",
                                                        "One-way delay as source", "One-way delay as destination"))

    direction = {1: 'src', 2: 'dest'}

    alltests['dt'] = pd.to_datetime(alltests['from'], unit='ms')

    # convert throughput bites to MB
    alltests.loc[alltests['idx']=='ps_throughput', 'value'] = alltests[alltests['idx']=='ps_throughput']['value'].apply(lambda x: round(x/1e+6, 2))

    # extract the data relevant for the given site name
    ips = metaDf[(metaDf['site']==site_name) | (metaDf['netsite']==site_name)]['ip'].values
    ip_colors = {ip: color for ip, color in zip(ips, colors)}


    legend_names = set()

    for col in [1,2]:
        measures = alltests[alltests[direction[col]].isin(ips)].copy().sort_values('from', ascending=False)

        for i, ip in enumerate(ips):
            # The following code sets the visibility to True only for the first occurrence of an IP
            first_time_seen = ip not in legend_names
            not_on_legend = True
            legend_names.add(ip)

            throughput = measures[(measures[direction[col]]==ip) & (measures['idx']=='ps_throughput')]
            
            if not throughput.empty:
                showlegend = first_time_seen==True and not_on_legend==True
                not_on_legend = False
                fig.add_trace(
                    go.Scattergl(
                        x=throughput['dt'],
                        y=throughput['value'],
                        mode='markers',
                        marker=dict(
                            color=ip_colors[ip]),
                        name=ip,
                        yaxis="y1",
                        legendgroup=ip,
                        showlegend=showlegend),
                    row=1, col=col
                )
            else: fig.add_trace(
                    go.Scattergl(
                        x=throughput['dt'],
                        y=throughput['value']),
                    row=1, col=col
                )
            
            packetloss = measures[(measures[direction[col]]==ip) & (measures['idx']=='ps_packetloss')]
            if not packetloss.empty:
                showlegend = first_time_seen==True and not_on_legend==True
                not_on_legend = False
                fig.add_trace(
                    go.Scattergl(
                        x=packetloss['dt'],
                        y=packetloss['value'],
                        mode='markers',
                        marker=dict(
                            color=ip_colors[ip]),
                        name=ip,
                        yaxis="y1",
                        legendgroup=ip,
                        showlegend=showlegend),
                    row=2, col=col
                )
            else: fig.add_trace(
                    go.Scattergl(
                        x=packetloss['dt'],
                        y=packetloss['value']),
                    row=2, col=col
                )

            owd = measures[(measures[direction[col]]==ip) & (measures['idx']=='ps_owd')]
            if not owd.empty:
                showlegend = first_time_seen==True and not_on_legend==True
                not_on_legend = False
                fig.add_trace(
                    go.Scattergl(
                        x=owd['dt'],
                        y=owd['value'],
                        mode='markers',
                        marker=dict(
                            color=ip_colors[ip]),
                            
                        name=ip,
                        yaxis="y1",
                        legendgroup=ip,
                        showlegend=showlegend),
                    row=3, col=col
                )
            else: fig.add_trace(
                    go.Scattergl(
                        x=owd['dt'],
                        y=owd['value']),
                    row=3, col=col
                )


    fig.update_layout(
            showlegend=True,
            title_text=f'{site_name} network measurements',
            legend=dict(
                traceorder="normal",
                font=dict(
                    family="sans-serif",
                    size=12,
                ),
            ),
            height=900,
        )


    # Update yaxis properties
    fig.update_yaxes(title_text=units['ps_throughput'], row=1, col=col)
    fig.update_yaxes(title_text=units['ps_packetloss'], row=2, col=col)
    fig.update_yaxes(title_text=units['ps_owd'], row=3, col=col)
    fig.layout.template = 'plotly_white'
    # py.offline.plot(fig)

    return fig


@timer
def groupAlarms(pivotFrames, metaDf, dateFrom):
    nodes = metaDf[~(metaDf['site'].isnull()) & ~(
        metaDf['site'] == '') & ~(metaDf['lat'] == '') & ~(metaDf['lat'].isnull())]
    alarmCnt = []

    for site, lat, lon in nodes[['site', 'lat', 'lon']].drop_duplicates().values.tolist():
        for e, df in pivotFrames.items():
            sdf = df[(df['tag'] == site) & ((df['to'] >= dateFrom) | (df['from'] >= dateFrom))]
            if not sdf.empty:
                entry = {"event": e, "site": site, 'cnt': len(sdf),
                        "lat": lat, "lon": lon}
            else:
                entry = {"event": e, "site": site, 'cnt': 0,
                        "lat": lat, "lon": lon}
            alarmCnt.append(entry)

    return pd.DataFrame(alarmCnt)


@timer
# '''Takes selected site from the Geo map and generates a Dash datatable'''
def generate_tables(site):
    global dateFrom
    global dateTo
    global frames
    global pivotFrames
    global alarmCnt

    out = []
    alarms4Site = alarmCnt[alarmCnt['site'] == site]

    if alarms4Site['cnt'].sum() > 0:
        for event in sorted(alarms4Site['event'].unique()):
            eventDf = pivotFrames[event]
            # find all cases where selected site was pinned in tag field
            ids = eventDf[(eventDf['tag'] == site) & ((eventDf['to'] >= dateFrom) | (eventDf['from'] >= dateFrom))]['id'].values

            tagsDf = frames[event]
            dfr = tagsDf[tagsDf.index.isin(ids)]
            dfr = alarmsInst.formatDfValues(dfr, event).sort_values('to', ascending=False)

            if len(ids):
                element = html.Div([
                    html.H3(event.upper()),
                    dash_table.DataTable(
                        data=dfr.to_dict('records'),
                        columns=[{"name": i, "id": i, "presentation": "markdown"} for i in dfr.columns],
                        markdown_options={"html": True},
                        id='tbl',
                        page_size=20,
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
                        filter_action="native",
                        sort_action="native",
                    ),
                ], className='single-table')

                out.append(element)
    else:
        out = html.Div(html.H3('No alarms for this site in the past day'), style={'textAlign': 'center'})

    return out


dash.register_page(__name__, path='/')

pq = Parquet()
alarmsInst = Alarms()
dateFrom, dateTo = hp.defaultTimeRange(1)


def layout(**other_unknown_query_strings):
    global dateFrom
    global dateTo
    global frames
    global pivotFrames
    global eventCnt
    global alarmCnt

    dateFrom, dateTo = hp.defaultTimeRange(1)
    print("Overview for period:", dateFrom," - ", dateTo)
    # dateFrom, dateTo = ['2022-12-11 09:40', '2022-12-11 21:40']
    frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
    metaDf = getMetaData()
    alarmCnt = groupAlarms(pivotFrames, metaDf, dateFrom)
    eventCnt = countDistEvents(alarmCnt)

    print(f'Number of alarms: {len(alarmCnt)}')
    print(f'Alarm types: {pivotFrames.keys()}')

    return html.Div(
            dbc.Row([
                dbc.Row([
                    dbc.Col(id='selected-site', className='cls-selected-site', align="start"),
                    dbc.Col(html.H2(f'Alarms reported in the past 24 hours ({dateTo} UTC)'), className='cls-selected-site')
                ], align="start", className='boxwithshadow mb-2 g-0'),
                dbc.Row(
                    dcc.Loading(
                        dcc.Graph(figure=builMap(eventCnt), id='site-map',
                                  className='cls-site-map boxwithshadow page-cont mb-2'),
                    ), className='g-0'
                ),
                dbc.Row([
                         dbc.Col(
                             dcc.Loading(
                                html.Div(id='datatables', children=[], className='datatables-cont'),
                             color='#00245A'),
                        width=12)
                    ],   className='site boxwithshadow page-cont mb-2 g-0', justify="center", align="center"),
                dbc.Row([
                         dbc.Col(
                             dcc.Loading(
                                 dcc.Graph(id="site-plots-in-out", className="site-plots site-inner-cont p-05"),
                             color='#00245A'),
                        width=12)
                    ],   className='site boxwithshadow page-cont mb-2 g-0', justify="center", align="center"),
                html.Div(id='page-content-noloading'),
                html.Br(),
                
            ], className='g-0', align="start", style={"padding": "0.5% 1.5%"}), className='main-cont')




# # '''Takes selected site from the Geo map and displays the relevant information'''
@dash.callback(
    [
        Output('datatables', 'children'),
        Output('selected-site', 'children'),
        Output('site-plots-in-out', 'figure'),
    ],
    Input('site-map', 'clickData')
)
def display_output(value):
    global eventCnt

    if value is not None:
        location = value['points'][0]['customdata'][0]
    else:
        location = eventCnt[eventCnt['cnt'] == eventCnt['cnt'].max()]['site'].values[0]
    
    return [generate_tables(location), html.H1(f'Selected site: {location}'), SitesOverviewPlots(location)]
