#DONE: change bows (up and down) DONE
#DONE: add title that this are T1 sites last 2 hours
#DONE: increase map, buttons for increasing the map
#DONE: initial loading
#DONE: change loading widget

#TODO: aggregation of records(I'm not aggregating, the query is bad but represents what I want to do)
#TODO: make a switch between latency, traceroute, packetloss?
#TODO: visualise meshes of testing 
#TODO: create it as perfSONAR toolkits status analytics page
#TODO: query once per 2 hours, don't query every time the picture is updated
#TODO: map size depending on a screen size

import json
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from pages.cric_extract_OPN import query_valid_trace_data

import utils.helpers as hp
from utils.parquet import Parquet
from utils.utils import buildMap, generateStatusTable

dash.register_page(
    __name__,
    path_template="/network-testing",
    title='Network testing',
    description="",
)
T1_NETSITES = [
    "BNL-ATLAS-LHCOPNE", "USCMS-FNAL-WC1-LHCOPNE", "RAL-LCG2",
    "RRC-KI-T1-LHCOPNE", "JINR-T1-LHCOPNE", "NCBJ-LHCOPN",
    "NLT1-SARA-LHCOPNE", "INFN-T1-LHCOPNE", "NDGF-T1-LHCOPNE",
    "KR-KISTI-GSDC-1-LHCOPNE", "IN2P3-CC-LHCOPNE", "pic-LHCOPNE",
    "FZK-LCG2-LHCOPNE", "CERN-PROD-LHCOPNE", "TRIUMF-LCG2-LHCOPNE", 'TW-ASGC'
]

# CRIC_NETSITES = ['CH-CERN', 'US-BNL', 'IT-INFN-CNAF', 'NL-NLT1-NIKHEF', 'US-CMS', 
#                'DE-KIT', 'FR-IN2P3', 'UK-RAL', 'NL-SURF-NREN', 'US-CIT', 'RU-JINR-T1', 
#                'ES-PIC', 'RU-RRC-KI-T1', 'NL-NLT1-SARA', 'CA-TRIUMF', 'EU-NDGF', 'KR-KISTI', 
#                'PL-NCBJ-CIS', 'CN-IHEP', 'TW-ASGC'] #in Elasticsearch I haven't found NL-NLT1-NIKHEF, NL-SURF-NREN, US-CIT, CN-IHEP
# cric_elastic_mapping = {'CH-CERN': 'CERN-PROD-LHCOPNE', 'US-BNL': 'BNL-ATLAS-LHCOPNE', 'IT-INFN-CNAF': 'INFN-T1-LHCOPNE',
#                         'US-CMS': 'USCMS-FNAL-WC1-LHCOPNE', 'DE-KIT': 'FZK-LCG2-LHCOPNE', 
#                         'FR-IN2P3': 'IN2P3-CC-LHCOPNE', 'UK-RAL': 'RAL-LCG2','RU-JINR-T1': 'JINR-T1-LHCOPNE', 'ES-PIC': 'pic-LHCOPNE',
#                         'RU-RRC-KI-T1': 'RRC-KI-T1-LHCOPNE', 'NL-NLT1-SARA': 'NLT1-SARA-LHCOPNE', 'CA-TRIUMF': 'TRIUMF-LCG2-LHCOPNE',
#                         'EU-NDGF': 'NDGF-T1-LHCOPNE', 'KR-KISTI': 'KR-KISTI-GSDC-1-LHCOPNE', 'PL-NCBJ-CIS': 'NCBJ-LHCOPN',
#                         'TW-ASGC': 'TW-ASGC'}
# def extract_addresses_cric():
#     # Load the JSON file (replace with the actual path to cric.json)
#     with open("cric.json", "r", encoding="utf-8") as f:
#         data = json.load(f)

#     # Extract relevant parts
#     header = data["header"]
#     body = data["body"]

#     # Find the index positions for the columns we need
#     subnets_index = header.index("Subnets")
#     lhcopn_limit_index = header.index("LHCOPN limit")

#     # Collect subnets where LHCOPN limit > 0
#     subnets_with_lhcopn = []
#     for row in body:
#         try:
#             limit = int(row[lhcopn_limit_index])
#         except ValueError:
#             continue  # skip if not a number
#         if limit > 0:
#             # Split multiple subnets in a cell
#             subnets = [s.strip() for s in row[subnets_index].split(",")]
#             subnets_with_lhcopn.extend(subnets)

#     # Output result
#     return subnets_with_lhcopn

# subnets_with_lhcopn = extract_addresses_cric()
# subnets_with_lhcopn = [ip_network(s.strip(), strict=False) for s in subnets_with_lhcopn]
# def query_records(date_from_str, date_to_str):
#     """
#     From Elasticsearch query all T1 sites ps_trace records.
#     """
#     allowed = set(T1_NETSITES)

#     q = {
#         "bool": {
#             "filter": [
#                 {
#                     "range": {
#                         "timestamp": {
#                             "gte": date_from_str,
#                             "lte": date_to_str,
#                             "format": "strict_date_optional_time"
#                         }
#                     }
#                 },
#                 # both ends must be in 'allowed'
#                 {"terms": {"src_netsite": list(allowed)}},
#                 {"terms": {"dest_netsite": list(allowed)}},
#             ]
#         }
#     }

#     try:
#         es_resp = hp.es.search(index='ps_trace', query=q, size=10000)
#         data = es_resp['hits']['hits']
#     except Exception as exc:
#         print(f"Failed to query ps_trace: {exc}")
#         data = []
#     return data

    # extracted = []
    # for item in data:
    #     src_ipv6 = item['_source'].get('source', {}).get('ipv6')
    #     dst_ipv6 = item['_source'].get('destination', {}).get('ipv6')
    #     src_ipv4 = item['_source'].get('source', {}).get('ipv4')
    #     dst_ipv4 = item['_source'].get('destination', {}).get('ipv4')
    #     all_sources = src_ipv6 + src_ipv4
    #     all_destinations = dst_ipv6 + dst_ipv4

    #     # src = (item['_source'].get('src_netsite') or '').upper()
    #     # dst = (item['_source'].get('dest_netsite') or '').upper()
    #     # src = (item['_source'].get('src_netsite') or '').upper()
    #     # dst = (item['_source'].get('dest_netsite') or '').upper()
    #     # if(src in chosen and dst in allowed) or (src in allowed and dst in chosen):
    #     for ip in all_sources:
    #         if ip
    #     if any(
    #             ip and any(ip_address(ip) in net for net in subnets_with_lhcopn)
    #             for ip in all_sources
    #         ):
    #         extracted.append({
    #             'src_netsite': (item['_source'].get('src_netsite') or '').upper(),
    #             'dest_netsite': (item['_source'].get('dest_netsite') or '').upper(),
    #             'destination_reached': item['_source'].get('destination_reached'),
    #             'path_complete': item['_source'].get('path_complete'),
    #             'created_at': item['_source'].get('created_at')
    #         })
    # return pd.DataFrame(extracted)

def layout(**other_unknown_query_strings):
    HOURS = 2 
    date_to = datetime.utcnow()
    date_from = date_to - timedelta(hours=HOURS)
    date_to_str = date_to.strftime(hp.DATE_FORMAT)
    date_from_str = date_from.strftime(hp.DATE_FORMAT)

    pq = Parquet()
    alarm_cnt = pq.readFile('parquet/alarmsGrouped.parquet')
    _, sites_status = generateStatusTable(alarm_cnt)

    # initial data
    traceroute_records = query_valid_trace_data(hours=4)
    records_df = pd.DataFrame(traceroute_records)
    if not records_df.empty:
        def pct(series):
            total = len(series) or 1
            true_pct = (series == True).sum() / total * 100
            false_pct = (series == False).sum() / total * 100
            return {'True': str(round(true_pct, 2)), 'False': str(round(false_pct, 2))}

        records_df["pair"] = records_df["src_netsite"] + " → " + records_df["dest_netsite"]
        records_df["path_complete_stats"] = records_df["path_complete"]
        records_df["destination_reached_stats"] = records_df["destination_reached"]
        grouped = records_df.groupby('pair').agg({
            'src_netsite': 'first',
            'dest_netsite': 'first',
            'path_complete': lambda x: x.mode().iloc[0] if not x.mode().empty else False,
            'destination_reached': 'any',
            'path_complete_stats': lambda x: pct(x),
            'destination_reached_stats': lambda x: pct(x)
        }).reset_index()
    else:
        grouped = pd.DataFrame(columns=[
            'pair','src_netsite','dest_netsite','path_complete',
            'destination_reached','path_complete_stats','destination_reached_stats'
        ])
    traceroutes_dict = grouped.to_dict('records')
    # base_map = buildMap(sites_status)

    # UI
    return html.Div(children=[
            # Title
            dcc.Store(id="valid-data", data=traceroutes_dict),
            html.H1(id="network-testing-title",
                            children=f"T1/OPN Disconnections — Last {HOURS} Hours pc_trace Tests"
                f"({date_from.strftime('%Y-%m-%d %H:%M')} to {date_to.strftime('%Y-%m-%d %H:%M')} UTC)",
                            className="mt-3 mb-1"),

            # Controls row
            dcc.Loading(
                        id="loading-map",
                        type="default",
                        color='#00245A',
                        children=[
                            dbc.Row([
                                dbc.Col(
                                    dcc.Dropdown(
                                        multi=True,
                                        id="t1-site-filter",
                                        placeholder="Filter T1 sites…",
                                        options= T1_NETSITES,
                                        closeOnSelect=False
                                    ),
                                    md=8
                                ),
                                dbc.Col(
                                    html.Div([
                                        dbc.Button("Search", id="btn-search", className="me-2", n_clicks=0),
                                        dbc.Button("Select all", id="btn-select-all", className="me-2", n_clicks=0),
                                        dbc.Button("Clear", id="btn-clear", color="secondary", n_clicks=0),
                                    ]),
                                    md=4, className="d-flex align-items-center justify-content-md-end mt-2 mt-md-0"
                                )
                            ], className="mb-2"),

                            # Graph
                            dbc.Row(
                                dbc.Col(
                                    dcc.Graph(id='traceroute-map', figure=buildMap(sites_status, True, grouped),
                                            style={'height': '85vh'}),
                                        width=12
                                    )
                            ),

                            # Store the current time window so callbacks can recompute consistently
                            dcc.Store(id="time-window", data={
                                "from": date_from_str, "to": date_to_str, "hours": HOURS
                            }),
                            dcc.Store(id="t1-selected", data=T1_NETSITES)
                        ]
                )
        ], className="p-1 site boxwithshadow page-cont m-3"
    )

# @dash.callback(
#     [
#         Output("t1-site-filter", "children")
#     ],
    
#     [
#         Input("btn-select-all", "n_clicks"),
#         Input("btn-clear", "n_clicks"),
#     ],

#     prevent_initial_call=True
# )
# def update_filter_values(select_all_btn, clear_btn):
#     if dash.callback_context.triggered[0]['prop_id'].split('.')[0] == 'btn-clear':
#         return []
#     return T1_NETSITES


# --- Rebuild the figure when the selection changes ---
@dash.callback(
    [
    Output("traceroute-map", "figure"),
     Output("t1-selected", "data")
    ],
    
    [
        Input("btn-search", "n_clicks"),
        Input("btn-select-all", "n_clicks"),
        Input("btn-clear", "n_clicks"),
        Input("t1-site-filter", "value")
    ],
    
    State("time-window", "data"),
    State("valid-data", "data"),
    prevent_initial_call=True
)
def update_map(search_btn, select_all_btn, clear_btn, selected_sites, tw, df):
    print("UPDATING MAP...")
    ctx = dash.callback_context.triggered[0]['prop_id']
    print("selected_sites")
    print(selected_sites)
    print("dash.callback_context.triggered")
    print(ctx)
    if not ('n_clicks' in ctx):
        print("dash.no_update")
        return dash.no_update, dash.no_update

    button = ctx.split('.')[0]
    print("button")
    print(button)
    
    date_from_str, date_to_str = tw["from"], tw["to"]

    pq = Parquet()
    alarm_cnt = pq.readFile('parquet/alarmsGrouped.parquet')
    _, sites_status = generateStatusTable(alarm_cnt)

    if button == 'btn-clear':
        return buildMap(sites_status), T1_NETSITES
    elif button == 'btn-select-all':
        selected_sites = T1_NETSITES
    
    # df = query_records(date_from_str, date_to_str, allowed_sites=selected_sites)
    df = pd.DataFrame(df)
    if selected_sites:
        df = df[df["src_netsite"].isin(selected_sites) | df["dest_netsite"].isin(selected_sites)]
    if df.empty or not selected_sites:
        print(f"NO TEST DATA for filter_values: {selected_sites}")
        return buildMap(
            sites_status, True,
            pd.DataFrame(columns=[
                'pair','src_netsite','dest_netsite','path_complete',
                'destination_reached','path_complete_stats','destination_reached_stats'
            ])
        ), T1_NETSITES

    def pct(series):
        total = len(series) or 1
        return {'True': str(round((series == True).sum() / total * 100, 2)),
                'False': str(round((series == False).sum() / total * 100, 2))}
    df["pair"] = df["src_netsite"] + " → " + df["dest_netsite"]
    df["path_complete_stats"] = df["path_complete"]
    df["destination_reached_stats"] = df["destination_reached"]
    grouped = df.groupby('pair').agg({
        'src_netsite': 'first',
        'dest_netsite': 'first',
        'path_complete': lambda x: x.mode().iloc[0] if not x.mode().empty else False,
        'destination_reached': 'any',
        'path_complete_stats': lambda x: pct(x),
        'destination_reached_stats': lambda x: pct(x)
    }).reset_index()
    
    
    return buildMap(sites_status, True, grouped), list(set(T1_NETSITES)-set(selected_sites))



