import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_table
import plotly.graph_objects as go
import pandas as pd

import view.templates as tmpl
import model.queries as qrs
import utils.helpers as hp
from model.DataLoader import PrtoblematicPairsDataLoader

class ProblematicPairsPage(object):

    indx_dict = {'all': 'All', 'ps_packetloss': 'Packet loss', 'ps_owd': 'One-way delay',
             'ps_retransmits': 'Retransmits', 'ps_throughput': 'Throughput'}

    problem_types = {'high_sigma':'The most problematic pairs', 'has_bursts':'Pairs showing bursts',
            'all_packets_lost': 'Pairs with 100% packets lost'}

    def __init__(self):
        self.obj = PrtoblematicPairsDataLoader()
        self.problems = self.obj.df.copy()
        # problems = pd.read_csv('df.csv')
        self.problems = self.problems.round(3)

        self.high_sigma = self.problems[(self.problems['high_sigma'] == 1) & (self.problems['all_packets_lost'] != 1) & ((self.problems['src_not_in'] == 0) | (self.problems['dest_not_in'] == 0))].sort_values(by=['zscore', 'doc_count'], ascending=False)
        self.has_bursts = self.problems[(self.problems['has_bursts'] == 1) & ((self.problems['src_not_in'] == 0) | (self.problems['dest_not_in'] == 0))].sort_values(by=['max_hash_zscore', 'doc_count'], ascending=False)
        self.all_packets_lost = self.problems[(self.problems['all_packets_lost'] == 1) & ((self.problems['src_not_in'] == 0) | (self.problems['dest_not_in'] == 0))].sort_values(by=['all_packets_lost', 'doc_count'], ascending=False)


    def getPairs(self, pair_type, row):
        if pair_type == 'host':
            source = row[row[['host_src', 'src']].first_valid_index()]
            destination = row[row[['host_dest', 'dest']].first_valid_index()]
            return str(source +' --> '+ destination)
        elif pair_type == 'site':
            src_site = row['site_src']
            dest_site = row['site_dest']
            if src_site != src_site:
                src_site = 'N/A'
            if dest_site != dest_site:
                dest_site = 'N/A'
            return src_site +' --> '+ dest_site


    def generateTable(self, item, df, num_rows):
        df = df[df['idx']==item][['host_src', 'src', 'site_src', 'host_dest', 'dest', 'site_dest', 'measures', 'value']]
        display_columns = {'host_src': 'host_src', 'src': 'src', 'site_src': 'site_src', 'host_dest': 'host_dest',
                           'dest': 'dest', 'site_dest': 'site_dest', 'measures': 'measures', 'value': hp.getValueUnit(item)}
        return  dash_table.DataTable(
                    id={'type': 'problem-table','index': item},
                    columns=[{"name": v, "id": k} for k,v in display_columns.items()],
                    data=df.to_dict("rows"),
                    style_header=tmpl.host_table_header,
                    style_cell={'font-family':'sans-serif', 'font-size': '11px'},
                    style_cell_conditional=tmpl.gen_info_table_cell,
                    sort_action='native',
                    filter_action='native',
                    page_size=10
                )


    def showProblems(self, idx, df):
        if idx == 'all':
            return dbc.Row([
                        dbc.Col([
                            html.H2(self.indx_dict[item], className="index-title"),
                            self.generateTable(item, df)
                        ], width=12) for item in hp.INDECES
                   ], justify="center")
        else:
            return dbc.Row([
                        dbc.Col(self.generateTable(idx, df))
                   ], justify="center")


    def createLayout(self):
        return html.Div([
                dbc.Row([
                     dbc.Col(
                        dcc.Tabs(id='tabs-prob-types', className='prob-types', value='high_sigma',
                                 children=[
                                    dcc.Tab(label=v.upper(), value=k) for k, v in self.problem_types.items()
                                ])
                     , width={"size": 8, "offset": 2})
                ]),
                dbc.Row([
                    dbc.Col([
                        dcc.Tabs(
                            id='tabs-indeces', className='indeces-types', value='all',
                            children=[
                                    dcc.Tab(label=v.upper(), value=k) for k, v in self.indx_dict.items()],
                                             vertical=True, style={'float': 'left'}),
                        dbc.Row([
                            dbc.Col([
                                dcc.Interval(
                                        id='update-interval-component',
                                        interval=60*60000, # 1hour
                                        n_intervals=0,
                                        max_intervals=-1,
                                    ),
                                html.Div(id="last-updated", className='last-updated')
                            ], align="center"),
                        ], align="center"),
                    ], width={"size": 2}),
                    dbc.Col([
                        dcc.Loading(
                            id="loading-problems",
                            type="default",
                            className="loading-problems",
                            children=html.Div("Loading may take a few minutes. Refresh the page to get the data from an hour ago.", id="tabs-content", className="problems-tabs-content")
                        ),
                    ], width=9)
                ])
            ])