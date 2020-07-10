import threading
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

import pandas as pd
import numpy as np
import time
from functools import reduce

import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import dash_html_components as html

import model.queries as qrs
from model.DataLoader import SiteDataLoader


sobj = SiteDataLoader()
sites = sobj.sites

def generateIPTable(site, test_method, df):
    ipv6 = len(df[(df['is_ipv6']==True)])
    ipv4 = len(df[(df['is_ipv6']==False)])
    return html.Table(
                # Header
                [html.Tr([html.Th(f"{test_method.capitalize()} hosts", colSpan='4', className=f"{test_method}-head")]) ] +
                # Body
                [html.Tr([
                    html.Td([col], colSpan='2', className='ipv-type') for col in ['IPv4', 'IPv6']
                ]) ] +
                [html.Tr([
                    html.Td([col], colSpan='2', className='ipv-count') for col in [ipv4, ipv6]
                ]) ],
                className='ip-table'
            )


def get2dayValue(dff, site, test_type):
    val = dff['day_val']
    if (len(val.values) > 0) and (val.isnull().values.any() == False):
        if test_type == 'throughput':
            return round(dff['day_val'].values[0]/1e+6, 2)
        return dff['day_val'].values[0]
    else: return 'N/A'


def getChangeVals(dfm, direction, site):
    if len(dfm[['day-2', 'day-1', 'day']].values) > 0:
        vals = dfm[['day-2', 'day-1', 'day']].values[0]
        data = dfm[['day-2', 'day-1', 'day']]
        vals = list(data.applymap(lambda x: "+"+str(x) if x>0 else x).values[0])
        vals.insert(0, direction)
        return vals
    else: return [direction, 'N/A', 'N/A', 'N/A'] 


def getUnit(test_type):
    if (test_type in ['packetloss', 'retransmits']):
        return 'packets'
    elif (test_type == 'throughput'):
        return 'MBps'
    elif (test_type == 'owd'):
        return 'ms'


def generateTable(site, test_type, df, dates):
    # there is no value for the first day of the period since
    # there is no data for the previous day to comapre with
    dates[0] = ''
    meas_type = (test_type).upper()

    dfin = df[df['direction']=='IN']
    dfout = df[df['direction']=='OUT']

    ch_in = getChangeVals(dfin, 'IN', site)
    ch_out = getChangeVals(dfout, 'OUT', site)

    val_in = get2dayValue(dfin, site, test_type)
    val_out = get2dayValue(dfout, site, test_type)

    unit = getUnit(test_type)

    return html.Table(
                # Header
                [html.Tr([html.Th(f"{meas_type} ({unit})", colSpan='4', className=f"{test_type}-tests")]) ] +
                # Body
                [html.Tr([
                    html.Td([col], colSpan='2', className='inout-type') for col in ['TODAY IN', 'TODAY OUT']
                ]) ] +
                [html.Tr([
                    html.Td(val_in, colSpan='2', className='inout-value'),
                    html.Td(val_out, colSpan='2', className='inout-value')
                ]) ]+
                [html.Tr(
                    html.Td('Change over the past 3 days (%)', colSpan='4', className='change-title'),
                ) ] +
                [html.Tr([
                    html.Td(col) for col in dates
                    ], className='change-values') ] +
                [html.Tr([
                    html.Td(col) for col in ch_in
                    ], className='change-values') ] +
                [html.Tr([
                    html.Td(col) for col in ch_out
                    ], className='change-values') ], className=f'{test_type}'
            )


def createCard(site):
        return dbc.Card(
                dbc.CardBody(
                    [
                        dbc.Row([
                                dbc.Col([
                                        html.H4(site, id=f"{site}-title", className='site-title')
                                ], width=12)
                        ], justify="left"),
                        dbc.Row([
                            dbc.Col([
                                     generateIPTable(site, 'latency', sobj.latency_df_related_only[sobj.latency_df_related_only['site']==site]),
                                     generateTable(site, 'packetloss', sobj.pls_data[sobj.pls_data['site']==site], sobj.pls_dates),
                            ], width=6),
                            dbc.Col([
                                     generateIPTable(site, 'throughput', sobj.throughput_df_related_only[sobj.throughput_df_related_only['site']==site]),
                                     generateTable(site, 'throughput', sobj.thp_data[sobj.thp_data['site']==site], sobj.thp_dates)
                            ], width=6),
                       ]),
                        dbc.Row([
                            dbc.Col([
                                     generateTable(site, 'owd', sobj.owd_data[sobj.owd_data['site']==site], sobj.owd_dates)
                            ], width=6),
                            dbc.Col([
                                     generateTable(site, 'retransmits', sobj.rtm_data[sobj.rtm_data['site']==site], sobj.rtm_dates)
                            ], width=6),
                        ])
                    ]
                ), id=str(site+'-card')
            )