import dash
from dash import Dash, dash_table, dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

from elasticsearch.helpers import scan

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import pandas as pd
import traceback

from utils.parquet import Parquet
from model.Updater import ParquetUpdater
import utils.helpers as hp
from utils.helpers import timer



@timer
def getAlarms(period, metaDf, taggedNodes):
    q = {
      "query" : {
        "bool": {
          "must": [
              {
                "range" : {
                  "created_at" : {
                    "from" : period[0],
                    "to": period[1]
                  }
                }
              },
              {
              "term" : {
                "category" : {
                  "value" : "Networking",
                  "boost" : 1.0
                }
              }
            }
          ]
        }
      }
    }
    result = scan(client=hp.es,index='aaas_alarms',query=q)
    data = {}
    
    sites = {}
    for item in result:
        event = item['_source']['event']
        temp = []
        if event in data.keys():
            temp = data[event]
        else: print(event)

        desc = item['_source']['source']
        tags = item['_source']['tags']
        desc['tag'] = tags
        temp.append(item['_source']['source'])
        data[event] = temp
        
        for t in tags:
            if event in taggedNodes:
                site = metaDf[metaDf['host']==t]['site'].values[0]
                print(metaDf[metaDf['host']==t][['host','site']].values)
                t = site

            if t in sites.keys():
                if event not in sites[t]:
                    temp = sites[t]
                    temp.append(event)
                    sites[t] = temp
            else: sites[t] = [event] 

    return [data, sites]



@timer
def getMetaData():
    meta = []
    data = scan(hp.es, index='ps_alarms_meta')
    for item in data:
        meta.append(item['_source'])

    if meta:
        return pd.DataFrame(meta)
    else: print('No metadata!')


@timer
def unpackAlarms(data, metaDf, taggedNodes):
    events = ['path changed', 'large clock correction', 'high packet loss', 'high packet loss on multiple links',
            'complete packet loss', 'bandwidth decreased from/to multiple sites', 'bandwidth decreased',
            'bandwidth increased from/to multiple sites', 'bandwidth increased', 'destination cannot be reached from any',
            'source cannot reach any', 'destination cannot be reached from multiple', 'firewall issue']

    # print(data.keys())
    frames, pivotFrames = {},{}


    def list2rows(df):
        s = df.apply(lambda x: pd.Series(x['tag']),axis=1).stack().reset_index(level=1, drop=True)
        s.name = 'tag'
        df = df.drop('tag', axis=1).join(s)

        return df


    def list2str(vals, sign):
        # print(vals.values)
        values = vals.values
        temp = ''
        for i, s in enumerate(values[0]):
            temp += f'{s}: {sign}{values[1][i]}% \n'

        return temp


    sign = {'bandwidth increased from/to multiple sites': '+',
            'bandwidth decreased from/to multiple sites': '-'}

    try:
        for e in events:
            if e in data.keys():
                df =  pd.DataFrame(data[e])
                df['tag_str'] = df['tag'].apply(lambda x: ', '.join(x))
                
                if 'sites' in df.columns:
                    df['sites'] = df['tag'].apply(lambda x: ' \n '.join(x))
                
                if 'dest_change' in df.columns:
                    df['change']=df[['dest_sites','dest_change']].apply(lambda x: list2str(x, sign[e]), axis=1)
                if 'src_change' in df.columns:
                    df['change']=df[['dest_sites','dest_change']].apply(lambda x: list2str(x, sign[e]), axis=1)


                frames[e] = df

                df = list2rows(df)
                df['id'] = df.index
                metaDf = metaDf[(metaDf['site']!='')&(metaDf['lat']!='')].sort_values(['site','lat'], na_position='last').drop_duplicates()

                if e not in taggedNodes:
                    df = pd.merge(metaDf[['site','lat','lon']], df, left_on='site', right_on='tag', how='right')
                else:
                    df = pd.merge(metaDf[['host','lat','lon','site']], df, left_on='host', right_on='tag', how='right')

                if 'site_x' in df.columns:
                    df = df.drop(columns=['site_x']).rename(columns={'site_y': 'site'})

                pivotFrames[e] = df
    except Exception as e:
        print(traceback.format_exc())

    return [frames, pivotFrames]


def groupAlarms(sites, metaDf):
    alarmCnt = []
    for s,v in sites.items():
        if s:
            alarmCnt.append({'site': s, 'cnt': len(v), 'events': v})
    alarmCnt = pd.DataFrame(alarmCnt)
    alarmCnt = pd.merge(metaDf[['site','lat','lon']].drop_duplicates(), alarmCnt, left_on='site', right_on='site', how='left')
    alarmCnt = alarmCnt[(alarmCnt['lat']!='')&(alarmCnt['lon']!='')&(alarmCnt['site']!='')]
    alarmCnt = alarmCnt.fillna(0)

    return alarmCnt



def builMap():
    alarmCnt['lat'] = alarmCnt['lat'].astype(float)
    alarmCnt['lon'] = alarmCnt['lon'].astype(float)

    fig = px.scatter_mapbox(data_frame=alarmCnt, lat="lat", lon="lon",
                            color="cnt", 
                            size="cnt",
                            color_continuous_scale=px.colors.sequential.deep,
                            size_max=10,
                            hover_name="site",
                            custom_data=['site', 'events'],
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

    fig.update_layout(template='plotly_white', title=f'Distinct events/alarms for the period: {dateFrom} - {dateTo}')
    # fig.update_annotations(font_size=12)
    # py.offline.plot(fig)

    return fig


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
                  '#c3b369', '#e1cc55', '#fee838', '#3e6595', '#4adfe1', '#b14ae1']

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





dash.register_page(__name__, path='/')

# cache the data needed for the overview charts. Run the code on the background every 2 min and store the data in /parquet.
# ParquetUpdater()

# most alarms tag sites, but some tag nodes instead
taggedNodes = ['large clock correction']

pq = Parquet()
metaDf = getMetaData()

dateFrom, dateTo = hp.defaultTimeRange(1)
# dateFrom, dateTo = ['2022-05-25 09:40', '2022-05-25 21:40']
data, sites = getAlarms(hp.GetTimeRanges(dateFrom, dateTo), metaDf, taggedNodes)

frames, pivotFrames = unpackAlarms(data, metaDf, taggedNodes)
alarmCnt = groupAlarms(sites, metaDf)



layout = html.Div(
            dbc.Row([
                dbc.Row(
                    dcc.Loading(
                        dcc.Graph(figure=builMap(), id='site-map', className='cls-site-map boxwithshadow page-cont mb-2'),
                    ), className='g-0'
                ),
                dbc.Row([
                         dbc.Col(dcc.Loading(id='selected-site'), className='cls-selected-site page-cont')
                         ],align="center",   className='boxwithshadow page-cont mb-2 g-0'),
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




# '''Takes selected site from the Geo map and displays the relevant information'''
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
    else: location = list(sites)[42]
    print('--------------------------',len(sites))
    measures = pq.readFile('parquet/raw/measures.parquet')
    return [generate_tables(location), html.H1(f'Selected site: {location}'), SitesOverviewPlots(location, 'src', metaDf, measures), SitesOverviewPlots(location, 'dest', metaDf, measures)]




# '''Takes selected site from the Geo map and generates a Dash datatable'''
def generate_tables(site):
    out = []

    if alarmCnt[alarmCnt['site']==site]['cnt'].values[0] > 0:
        for event in sorted(sites[site]):
            eventDf = pivotFrames[event]

            # find all cases where selected site was pinned in tag field
            if event not in taggedNodes:
                ids = eventDf[eventDf['tag']==site]['id'].values
            else: ids = eventDf[eventDf['site']==site]['id'].values

            tagsDf = frames[event]
            dfr = tagsDf[tagsDf.index.isin(ids)]
                
            columns = dfr.columns.tolist()
            columns.remove('tag')
            if event in ['bandwidth decreased from/to multiple sites', 'bandwidth increased from/to multiple sites']:
                columns = [el for el in columns if el not in ['src_sites', 'dest_sites', 'src_change', 'dest_change']]

            element = html.Div([
                        html.H3(event.upper()),
                        dash_table.DataTable(
                            data=dfr[columns].to_dict('records'),
                            columns=[{"name": i, "id": i} for i in columns],
                            id='tbl',
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

            out.append(element)
    else: out = html.Div(html.H3('No events'))

    return out
