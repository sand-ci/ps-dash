import urllib.parse as urlparse
from urllib.parse import parse_qs
import utils.helpers as hp
import pandas as pd
import model.queries as qrs
import view.templates as tmpl
import numpy as np

import plotly.graph_objects as go
import plotly as py
import plotly.express as px
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
import dash_table
import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html

from view.problematic_pairs import ProblematicPairsPage


class PairPlotsPage(ProblematicPairsPage):

    def __init__(self, url):
        self.parent = ProblematicPairsPage()
        self.forward = self.getData(url, True)
        self.reversed = self.getData(url, False)


    def getData(self, url, forward):
        self._time_list = hp.GetTimeRanges(self.parent.obj.dateFrom, self.parent.obj.dateTo)
        parsed = urlparse.urlparse(url)

        self._idx = parse_qs(parsed.query)['idx'][0]

        if forward:
            # self._src and self._dest are the orginal values
            self._src = parse_qs(parsed.query)['src'][0]
            self._dest = parse_qs(parsed.query)['dest'][0]
            self._host_src = parse_qs(parsed.query)['src_host'][0]
            self._host_dest = parse_qs(parsed.query)['dest_host'][0]
            self._src_ip = self._src
            self._dest_ip = self._dest
        else:
            self._host_src = parse_qs(parsed.query)['dest_host'][0]
            self._host_dest = parse_qs(parsed.query)['src_host'][0]
            self._src_ip = parse_qs(parsed.query)['dest'][0]
            self._dest_ip = parse_qs(parsed.query)['src'][0]

        df = pd.DataFrame(qrs.queryAllValues(self._idx, self._src_ip, self._dest_ip, self._time_list))
        print(len(df))
        df.rename(columns={hp.getValueField(self._idx): 'value'}, inplace=True)
        if len(df) > 0:
            df['log_value'] = np.log10(df['value'].replace(0, np.nan))
            df['sqrt'] = df['value']**(1/2)
        return df


    def buildGraph(self, df):
        fig = go.Figure()
        if len(df) > 0:
            df = df.sort_values('timestamp', ascending=False)
            df['dt'] = pd.to_datetime(df['timestamp'], unit='ms')

            fig.add_trace(go.Scatter(x=df['dt'], y=df['value'],
                    mode='markers',
                    marker=dict(
                        color='navy'),
                    name='measures',
                    yaxis="y1"),
             )
            fig.add_trace(go.Scatter(x=df['dt'], y=df['sqrt'],
                                mode='markers',
                                marker=dict(
                                    color='#F03A47'),
                                name='sqrt',
                                yaxis="y2",
                                visible='legendonly'),
                         )

            fig.add_trace(go.Scatter(x=df['dt'], y=df['log_value'],
                                mode='markers',
                                marker=dict(
                                    color='#00BCD4'),
                                name='log',
                                yaxis="y3",
                                visible='legendonly'),
                         )
                

            fig.update_layout(
                xaxis=dict(
                    domain=[0.05, 0.9]
                ),
                yaxis1=dict(
                    title="measures",
                    anchor="free",
                    side="left",
                    position=0.05,
                    titlefont=dict(
                        color="navy"
                    ),
                    tickfont=dict(
                        color="navy"
                    )
                ),
                yaxis2=dict(
                    title="sqrt",
                    anchor="x",
                    overlaying="y",
                    side="right",
                    titlefont=dict(
                        color="#F03A47"
                    ),
                    tickfont=dict(
                        color="#F03A47"
                    ),
                ),
                yaxis3=dict(
                    title="log",
                    anchor="free",
                    overlaying="y",
                    side="right",
                    position=0.98,
                    titlefont=dict(
                        color="#00BCD4"
                    ),
                    tickfont=dict(
                        color="#00BCD4"
                    ),
                )
            )
            fig.update_layout(title=f'{self._idx}: Measures for {self._host_src} ⇒ {self._host_dest}',
                              template = 'plotly_white')

        else:
            fig.update_layout(title=f'{self._idx}: Measures for {self._host_src} ⇒ {self._host_dest}',
                              template = 'plotly_white',
                              annotations = [
                                {
                                    "text": "No data found",
                                    "xref": "paper",
                                    "yref": "paper",
                                    "showarrow": False,
                                    "font": {
                                        "size": 18
                                    }
                                }
                            ])
        return fig


    def phraseProblem(self, ptype, idx):
            if ptype == 'high_sigma' or ptype == 'all_packets_lost':
                phrase = 'overall'
            elif ptype == 'has_bursts':
                phrase = 'periods of'

            if idx == 'ps_throughput':
                return (f'The pair shows {phrase} low throughout')
            if idx == 'ps_retransmits':
                return (f'The pair shows {phrase} high number of retransmitted packages')
            if idx == 'ps_owd':
                return (f'The pair shows {phrase} high latency')
            if idx == 'ps_packetloss':
                return (f'The pair shows {phrase} high packetloss')


    def createCards(self):
        data = self.parent.problems[(self.parent.problems['src']==self._src) &
                                 (self.parent.problems['dest']==self._dest)].set_index('idx').to_dict('index')
        watch4 = ['high_sigma', 'all_packets_lost', 'has_bursts']

        '''Store the sentences in a dictionary'''
        ddict = {}
        for idx in data:
            for k, v in data[idx].items():
                if k in watch4 and v == 1:
                    ddict[idx] = {'text':self.phraseProblem(k, idx), 'avg':data[idx]['value']}

        '''Search for other problems for the same pair and show them. Otherwise return None'''
        other_indeces = [item for item in ddict.keys() if item != self._idx]
        if len(other_indeces) > 0:
            other_issues = html.Div([
                            html.Div([
                                html.Div(ddict[item]['text'], className="card-text"),
                                html.H2(f"{int(round(ddict[item]['avg'], 0))} {hp.getValueUnit(item)}", className="card-text")
                            ]) for item in other_indeces
                        ])
        else: other_issues = html.Div('None', className="card-text")

        return  dbc.Col(
                    html.Div([
                        html.H2('ISSUE', className="card-title"),
                        html.Div(ddict[self._idx]['text'], className="card-text"),
                        html.H2(f"{int(round(ddict[self._idx]['avg'], 0))} {hp.getValueUnit(self._idx)}", className="card-text")
                ], className='issue ppage-header'), width=3), dbc.Col(
                dbc.Row([
                    dbc.Col(
                        html.Div([
                            html.H2('SOURCE', className="card-title"),
                            html.Div(data[self._idx]['host_src'], className="card-text"),
                            html.Div(data[self._idx]['src'], className="card-text"),
                            html.Div(data[self._idx]['site_src'], className="card-text")
                     ], className='src-issue ppage-header'), width=5, className='issue-wrapper'), dbc.Col(
                        html.P(['⇒'], className='arrow-right'), width=0.5),dbc.Col(
                        html.Div([
                            html.H2('DESTINATION', className="card-title"),
                            html.Div(data[self._idx]['host_dest'], className="card-text"),
                            html.Div(data[self._idx]['dest'], className="card-text"),
                            html.Div(data[self._idx]['site_dest'], className="card-text")
                     ], className='dest-issue ppage-header'), width=5, className='issue-wrapper')
                ], justify="center", align="center", className='issue-wrapper'), width=6), dbc.Col(
                    html.Div([
                        html.H2('Other issues for the same pair', className="card-title"),
                        other_issues
                 ], className='other-issue ppage-header'), width=3)


    def createLayout(self):
        return html.Div([
                dbc.Row(
                    self.createCards(), className='issue-header', no_gutters=True, justify='center'
                ),
                dbc.Row([
                     dbc.Col(
                         html.Div([
                            dcc.Graph(figure=self.buildGraph(self.forward))
                        ])
                     ),
                    dbc.Col(
                         html.Div([
                            dcc.Graph(figure=self.buildGraph(self.reversed))
                        ])
                     )
                ])
              ])