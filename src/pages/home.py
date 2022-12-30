import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import pandas as pd


from model.Updater import ParquetUpdater
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
        return pd.Series([group['event'].unique(), group.lat.values[0], group.lon.values[0], len(group['event'].unique())])

    mapDf = alarmCnt[['site', 'event', 'lat', 'lon']].groupby('site').apply(groupByEvent).reset_index()
    mapDf.columns = ['site', 'event', 'lat', 'lon', 'cnt']
    mapDf['lat'] = mapDf['lat'].astype(float)
    mapDf['lon'] = mapDf['lon'].astype(float)

    return mapDf


@timer
def builMap(mapDf):
    fig = px.scatter_mapbox(data_frame=mapDf, lat="lat", lon="lon",
                            color="cnt", 
                            size="cnt",
                            color_continuous_scale=px.colors.sequential.deep,
                            size_max=10,
                            hover_name="site",
                            custom_data=['site', 'event'],
                            zoom=1
                        )
    fig.update_traces(
        hovertemplate="<br>".join([
            "Site: <b>%{customdata[0]}</b>",
            "Events: %{customdata[1]}",
        ])
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
    # fig.update_annotations(font_size=12)
    # py.offline.plot(fig)

    return fig


@timer
# Generates the 4 plot for the 
def SitesOverviewPlots(site_name, direction, metaDf, measures):
    units = {
        'ps_packetloss': 'packets',
        'ps_throughput': 'MBps',
        'ps_owd': 'ms',
        'ps_retransmits': 'packets'
        }

    colors = ['#720026', '#e4ac05', '#00bcd4', '#1768AC', '#ffa822', '#134e6f', '#ff6150', '#1ac0c6', '#492b7c', '#9467bd',
                '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
                '#c3b369', '#e1cc55', '#fee838', '#3e6595', '#4adfe1', '#b14ae1',
                '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
                '#720026', '#e4ac05', '#00bcd4', '#1768AC', '#ffa822', '#134e6f', '#ff6150', '#1ac0c6', '#492b7c', '#9467bd',
                '#1f77b4', '#ff7f0e', '#2ca02c','#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
                '#c3b369', '#e1cc55', '#fee838', '#3e6595', '#4adfe1', '#b14ae1',]

    # extract the data relevant for the given site name
    ips = metaDf[metaDf['site']==site_name]['ip'].values
    measures = measures[measures[direction].isin(ips)].sort_values('from', ascending=False)
    measures['dt'] = pd.to_datetime(measures['from'], unit='ms')
    # convert throughput bites to MB
    measures.loc[measures['idx']=='ps_throughput', 'value'] = measures[measures['idx']=='ps_throughput']['value'].apply(lambda x: round(x/1e+6, 2))
    
    fig = go.Figure()
    fig = make_subplots(rows=2, cols=2, subplot_titles=("Packet loss", "Throughput", 'One-way delay', 'Retransmits'))
    for i, ip in enumerate(ips):

        # The following code sets the visibility to True only for the first occurence of an IP
        visible = {'ps_packetloss': False, 
                'ps_throughput': False,
                'ps_owd': False,
                'ps_retransmits': False}

        if ip in measures[measures['idx']=='ps_packetloss'][direction].unique():
            visible['ps_packetloss'] = True
        elif ip in measures[measures['idx']=='ps_throughput'][direction].unique():
            visible['ps_throughput'] = True
        elif ip in measures[measures['idx']=='ps_owd'][direction].unique():
            visible['ps_owd'] = True
        elif ip in measures[measures['idx']=='ps_retransmits'][direction].unique():
            visible['ps_retransmits'] = True


        fig.add_trace(
            go.Scattergl(
                x=measures[(measures[direction]==ip) & (measures['idx']=='ps_packetloss')]['dt'],
                y=measures[(measures[direction]==ip) & (measures['idx']=='ps_packetloss')]['value'],
                mode='markers',
                marker=dict(
                    color=colors[i]),
                name=ip,
                yaxis="y1",
                legendgroup=ip,
                showlegend = visible['ps_packetloss']),
            row=1, col=1
        )

        fig.add_trace(
            go.Scattergl(
                x=measures[(measures[direction]==ip) & (measures['idx']=='ps_throughput')]['dt'],
                y=measures[(measures[direction]==ip) & (measures['idx']=='ps_throughput')]['value'],
                mode='markers',
                marker=dict(
                    color=colors[i]),
                name=ip,
                yaxis="y1",
                legendgroup=ip,
                showlegend = visible['ps_throughput']),
            row=1, col=2
        )

        fig.add_trace(
            go.Scattergl(
                x=measures[(measures[direction]==ip) & (measures['idx']=='ps_owd')]['dt'],
                y=measures[(measures[direction]==ip) & (measures['idx']=='ps_owd')]['value'],
                mode='markers',
                marker=dict(
                    color=colors[i]),
                name=ip,
                yaxis="y1",
                legendgroup=ip,
                showlegend = visible['ps_owd']),
            row=2, col=1
        )

        fig.add_trace(
            go.Scattergl(
                x=measures[(measures[direction]==ip) & (measures['idx']=='ps_retransmits')]['dt'],
                y=measures[(measures[direction]==ip) & (measures['idx']=='ps_retransmits')]['value'],
                mode='markers',
                marker=dict(
                    color=colors[i]),
                name=ip,
                yaxis="y1",
                legendgroup=ip,
                showlegend = visible['ps_retransmits'],
                ),
            row=2, col=2
        )


    fig.update_layout(
            showlegend=True,
            title_text=f'{site_name} as {"source" if direction == "src" else "destination"} of measures',
            legend=dict(
                traceorder="normal",
                font=dict(
                    family="sans-serif",
                    size=12,
                ),
            ),
            height=600,
        )


    # Update yaxis properties
    fig.update_yaxes(title_text=units['ps_packetloss'], row=1, col=1)
    fig.update_yaxes(title_text=units['ps_throughput'], row=1, col=2)
    fig.update_yaxes(title_text=units['ps_owd'], row=2, col=1)
    fig.update_yaxes(title_text=units['ps_retransmits'], row=2, col=2)

    fig.layout.template = 'plotly_white'
    # py.offline.plot(fig)

    return fig

import time 

@timer
def groupAlarms(pivotFrames, metaDf):
  nodes = metaDf[~(metaDf['site'].isnull()) & ~(
      metaDf['site'] == '') & ~(metaDf['lat'] == '') & ~(metaDf['lat'].isnull())]
  alarmCnt = []

  for site, lat, lon in nodes[['site', 'lat', 'lon']].drop_duplicates().values.tolist():
    for e, df in pivotFrames.items():
      sdf = df[(df['tag'] == site) & ((df['to'] >= dateFrom) | (df['from'] >= dateFrom))]
      if not sdf.empty:
        entry = {"event": e, "site": site, 'cnt': len(sdf),
                 "lat": lat, "lon": lon}
        alarmCnt.append(entry)

  return pd.DataFrame(alarmCnt)


@timer
# '''Takes selected site from the Geo map and generates a Dash datatable'''
def generate_tables(site):
    out = []
    alarms4Site = alarmCnt[alarmCnt['site'] == site]

    if len(alarms4Site) > 0:
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
                        columns=[{"name": i, "id": i}
                                for i in dfr.columns],
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
        out = html.Div(html.H3('No events'))

    return out


dash.register_page(__name__, path='/')

# cache the data needed for the overview charts. Run the code on the background every 2 min and store the data in /parquet.
ParquetUpdater()
pq = Parquet()


dateFrom, dateTo = hp.defaultTimeRange(1)
# dateFrom, dateTo = ['2022-12-11 09:40', '2022-12-11 21:40']
alarmsInst = Alarms()
frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
metaDf = qrs.getMetaData()

alarmCnt = groupAlarms(pivotFrames, metaDf)
eventCnt = countDistEvents(alarmCnt)
print(f'Number of alarms: {len(alarmCnt)}')
print(f'Alarm types: {pivotFrames.keys()}')


layout = html.Div(
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
                                 dcc.Graph(id="site-plots-out", className="site-plots site-inner-cont p-05"),
                             color='#00245A'),
                        width=12)
                    ],   className='site boxwithshadow page-cont mb-2 g-0', justify="center", align="center"),
                dbc.Row([
                         dbc.Col(
                             dcc.Loading(
                                 dcc.Graph(id="site-plots-in", className="site-plots site-inner-cont p-05"),
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
        Output('site-plots-out', 'figure'),
        Output('site-plots-in', 'figure'),
    ],
    Input('site-map', 'clickData')
)
def display_output(value):
    if value is not None:
        location = value['points'][0]['customdata'][0]
    else:
        location = eventCnt[eventCnt['cnt'] == eventCnt['cnt'].max()]['site'].values[0]
    measures = pq.readFile('parquet/raw/measures.parquet')
    return [generate_tables(location), html.H1(f'Selected site: {location}'), SitesOverviewPlots(location, 'src', metaDf, measures), SitesOverviewPlots(location, 'dest', metaDf, measures)]
