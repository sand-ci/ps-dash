import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly as py

import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import dash_html_components as html

import model.queries as qrs
import utils.helpers as hp
from model.DataLoader import SitesRanksDataLoader


class SitesPage():

    def __init__(self):
        sr = SitesRanksDataLoader()
        df = sr.df
        self.all_df = sr.all_df
        self.measures = sr.measures
        self.siteMap = self.createGeoMap(df)

    def createGeoMap(self, df):
        df = df[['site', 'rank1', 'size', 'lon', 'lat']].sort_values('rank1')
        df['lat'] = df['lat'].astype(float)
        df['lon'] = df['lon'].astype(float)

        fig = px.scatter_mapbox(df, lat="lat", lon="lon",
                                color="rank1", 
                                size="size",
                                color_continuous_scale=px.colors.sequential.Plasma[::-1],
                                size_max=8,
                                hover_name="site",
                                custom_data=['site', 'rank1'],
                                zoom=1,
                            )
        fig.update_traces(
            hovertemplate="<br>".join([
                "Site: <b>%{customdata[0]}</b>",
                "Rank: %{customdata[1]}",
            ])
        )

        fig.update_layout(
            margin={'l': 10, 'b': 0, 'r': 5, 't': 0},
            mapbox=dict(
                accesstoken=hp.mapboxtoken.strip(),
                bearing=0,
                center=go.layout.mapbox.Center(
                    lat=12,
                    lon=-6
                ),
                pitch=0,
                style='mapbox://styles/petyavas/ckhyzstu92b4c19r92w0o8ayt'
            ),
            coloraxis_colorbar=dict(
                title="Rank",
                thicknessmode="pixels",
                thickness=30,
                len=0.95,
            ),
            height = 450,
            )
        # py.offline.plot(fig)
        return fig

    def SitesOverviewPlots(self, site_name, direction):
        units = {'ps_packetloss': 'packets',
            'ps_throughput': 'MBps',
            'ps_owd': 'ms',
            'ps_retransmits': 'packets'}

        colors = ['#720026', '#e4ac05', '#00bcd4', '#1768AC', '#ffa822', '#134e6f', '#ff6150', '#1ac0c6', '#492b7c', '#1f77b4', '#ff7f0e', '#2ca02c', '#9467bd',
                 '#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74', '#c3b369', '#e1cc55', '#fee838']

        # extract the data relevant for the given site name
        ips = self.all_df[self.all_df['site']==site_name]['ip'].values
        measures = self.measures[self.measures[direction].isin(ips)].sort_values('from', ascending=False)
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


    def buildLayout(self):
        return html.Div(
            [
                    dbc.Row([
                         dbc.Col(
                             html.Div([
                                html.H2('Sites\' ranking based on their measures'),
                                html.P('The darker the color, the worse their performance. Smaller points indicate missing set of measures.', style={'font-size': '18px'})

                             ], className='sites-title-left'), align='start', width=7
                         ),
                        dbc.Col(
                             html.Div([
                                html.P('Click on a site in the map to see an overview over the past days')
                             ], className='sites-title-right'), align='end', width=5
                         )
                    ], className='sites-header', align="center", justify="between", no_gutters=True),
                    dbc.Row([
                         dbc.Col(
                             dcc.Loading(
                                id='site-details', className='site-report'
                            ),
                         width=4),
                        dbc.Col(
                            html.Div([
                                html.Div(children=[
                                dcc.Graph(figure=self.siteMap, id='site-map', className='site-graph',
                                          style={'height': '450px'}
                                         )
                            ])
                            ], className="site-details")
                             , width=8)
                    ],  className='aligned-row site-inner-cont', no_gutters=True, justify="center", align="center"),
                    ], className='site boxwithshadow page-cont'), dbc.Row([
                         dbc.Col(
                             dcc.Loading(
                                 dcc.Graph(id="site-plots-out", className="site-plots site-inner-cont"),
                             color='#00245A'),
                        width=12)
                    ],   className='site boxwithshadow page-cont', no_gutters=True, justify="center", align="center"),dbc.Row([
                         dbc.Col(
                             dcc.Loading(
                                 dcc.Graph(id="site-plots-in", className="site-plots site-inner-cont"),
                         color='#00245A'), width=12)
                    ],   className='site boxwithshadow page-cont', no_gutters=True, justify="center", align="center")

