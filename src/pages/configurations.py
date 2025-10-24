import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ALL
import pandas as pd
from datetime import datetime, timedelta

import utils.helpers as hp
from utils.parquet import Parquet
from utils.utils import buildMap, generateStatusTable, get_color
from dash import dash_table as dt
import numpy as np
import plotly.express as px

import urllib3
import aiohttp
import model.queries as qrs
from pathlib import Path
from datetime import datetime, timezone, timedelta
from ipaddress import ip_address, ip_network
from collections import defaultdict
from dash.exceptions import PreventUpdate
from collections import defaultdict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

dash.register_page(
    __name__,
    path_template="/network-testing",
    title='Network testing',
    description="",
)
T1_NETSITES = [
    "BNL-ATLAS-LHCOPNE", "USCMS-FNAL-WC1-LHCOPNE", "RAL-LCG2-LHCOPN",
    "JINR-T1-LHCOPNE", "NCBJ-LHCOPN",
    "NLT1-SARA-LHCOPNE", "INFN-T1-LHCOPNE", "NDGF-T1-LHCOPNE",
    "KR-KISTI-GSDC-1-LHCOPNE", "IN2P3-CC-LHCOPNE", "PIC-LHCOPNE",
    "FZK-LCG2-LHCOPNE", "CERN-PROD-LHCOPNE", "TRIUMF-LCG2-LHCOPNE"
]

PORT = 443
CONNECT_TIMEOUT = 3
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=5)
RETRIES = 3
BACKOFF = [0, 5, 15]
STATUSES = ["ACTIVE_HTTP", "ACTIVE_TCP_ONLY", "UNREACHABLE_CANDIDATE", "RETIRED_DNS"]
ES_INDICES = ["ps_trace", "ps_throughput", "ps_owd"]
LOOKBACK_DAYS = 30
TO_DATE = datetime.utcnow()
FROM_DATE = TO_DATE - timedelta(hours=2)


def readParquetToDf(pq, parquet_path):
    """
    """
    try: 
        print(f"Reading the parquet file {parquet_path}...")
        df = pq.readFile(parquet_path)
        return df
    except Exception as err:
        print(err)
        print(f"Problems with reading the file {parquet_path}")
        

def ip_in_any(ip, networks):
    return any(ip_address(ip) in ip_network(net) for net in networks)


def extract_groups_and_hosts(mesh_config, site=False, host=False):
    """
    Return:
      groups: list[str] (aligned with mesh_config["Groups"])
      members_by_group: dict[group_name] -> list[str hostnames]
      host_to_site: dict[host] -> netsite (or rcsite)
    """
    grouped = {}
    if site:
        unique_test_groups = (mesh_config[mesh_config['Site'] == site])["Groups"].unique()
    else:
        unique_test_groups = (mesh_config[mesh_config['Host'] == host])["Groups"].unique()
    for g in unique_test_groups:
        grouped[g] = []
    for i, row in mesh_config.iterrows():
        if row["Group"] in unique_test_groups:
            grouped[row["Group"]].append((row["Site"], row["Host"]))
    return grouped

def toolkits_overloaded_UI():
    configs_details = [
        # --- stores for interaction (already suggested) ---
        dcc.Store(id="psc-selected-host"),                     # clicked host

        # --- modal shell (closed by default) ---
        dbc.Modal(
            id="psc-modal",
            is_open=False,
            size="lg",
            scrollable=True,
            children=[
                dbc.ModalHeader(dbc.ModalTitle(id="psc-modal-title")),
                dbc.ModalBody(id="psc-modal-body"),
                dbc.ModalFooter(
                    dbc.Button("Close", id="psc-modal-close", className="ms-auto", n_clicks=0)
                ),
            ],
        ),
    ]
    
    # perfSONAR toolkits' overload
    psconfig_section = html.Div([
        html.Div(id="toolkits-overloaded", children=[
                html.Div(
                    id="toolkits-overloaded-header",
                    n_clicks=0,
                    style={"cursor": "pointer", "display": "flex", "alignItems": "center", "gap": "8px"},
                    children=[
                        html.I(id="toolkits-overloaded-caret", className="fas fa-chevron-right", style={"transition": "transform 0.2s ease"}),
                        html.H3("Overloaded Toolkits perfSONAR", className="mt-2"),
                    ]
                ),
                dbc.Collapse(
                    id={"type": "collapse", "id": "toolkits-overloaded-collapse"},
                    is_open=False,  # hidden by default
                    children=[
                            html.P(
                                "Counts of scheduled tests per perfSONAR host (from psConfig). "
                                "Spots potentially overloaded toolkits.",
                                className="text-secondary", style={"font-size": "1.3rem", "font-style": "italic"}
                            ),
                            html.Div(id="psc-unique-hosts-line", className="mb-2"),

                            # Controls
                            dbc.Row([
                                dbc.Col([
                                    html.Label("Netsite (optional)", className="small text-muted"),
                                    dcc.Dropdown(id="psc-filter-netsite", multi=True, placeholder="All")
                                ], md=2),
                                dbc.Col([
                                    html.Label("Host (optional)", className="small text-muted"),
                                    dcc.Dropdown(id="psc-filter-host", multi=True, placeholder="All")
                                ], md=2),
                                dbc.Col([
                                    html.Label("Highlight top N", className="small text-muted"),
                                    dcc.Input(id="psc-top-n", type="number", min=1, step=1, value=20, style={"width": "100%"})
                                ], md=2),
                                dbc.Col([
                                    html.Label("Show columns", className="small text-muted"),
                                    dcc.Checklist(
                                        id="psc-cols",
                                        options=[{"label": c, "value": c} for c in ["Throughput","Trace","Latency","Latencybg","Rtt"]],
                                        value=["Throughput","Trace","Latency","Latencybg","Rtt"],
                                        inline=True
                                    )
                                ], md=6),
                            ], className="mb-2"),

                            # Viz + table
                            dbc.Row([
                                dbc.Col(dcc.Graph(id="psc-bar-top"), md=6),
                                dbc.Col(html.Div(id="psc-table-wrap"), md=6)
                            ]),
                            *configs_details
                    ])
            ], className="p-1 site boxwithshadow m-3"),
        ])
    return psconfig_section

def layout(**other_unknown_query_strings):
    pq = Parquet()
    alarm_cnt = pq.readFile('parquet/alarmsGrouped.parquet')
    _, sites_status = generateStatusTable(alarm_cnt)

    # initial data
    traceroute_records = readParquetToDf(pq, 'parquet/raw/traceroutes_OPN.parquet')
    records_df = pd.DataFrame(traceroute_records)
    print("All unique sites names: ")
    print(set(list(records_df['dest_netsite'].unique()) + list(records_df['src_netsite'].unique())))
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
            'src_host': lambda x: list(set(x)),
            'dest_host': lambda x: list(set(x)),
            'path_complete': lambda x: x.mode().iloc[0] if not x.mode().empty else False,
            'destination_reached': 'any',
            'path_complete_stats': lambda x: pct(x),
            'destination_reached_stats': lambda x: pct(x)
        }).reset_index()
        grouped['color'] = grouped.apply(lambda row: get_color(row), axis=1)
    else:
        grouped = pd.DataFrame(columns=[
            'pair','src_netsite','dest_netsite', 'src_host', 'dest_host', 'path_complete',
            'destination_reached','path_complete_stats','destination_reached_stats', 'color'
        ])
    traceroutes_dict = grouped.to_dict('records')
    summary_df, missing_df = compute_connectivity_summaries(grouped, T1_NETSITES)
    print(" --- getting hosts from psConfigAdmin ---")
    mesh_config = readParquetToDf(pq, 'parquet/raw/psConfigData.parquet')
    all_hosts_in_configs = mesh_config['Host'].unique()
    print(f"All hosts in psConfig: {len(all_hosts_in_configs)}\n")
    

    print(" --- getting PerfSonars from WLCG CRIC ---")
    all_cric_perfsonar_hosts = readParquetToDf(pq, 'parquet/raw/CRICDataHosts.parquet')['host'].tolist()
    print(f"All perfSONAR hosts in CRIC: {len(all_cric_perfsonar_hosts)}\n")
              # or your read_psconfig_parquet
    mesh_exploded = explode_psconfig(mesh_config)
    stats_df, host_details = build_psc_stats_table(mesh_exploded)
    total_unique_hosts = mesh_exploded['Host'].nunique()
    
    # UI
    return html.Div(children=[
        dcc.Store(id="psc-exploded", data=mesh_exploded.to_dict("records")),
        dcc.Store(id="psc-host-stats", data=stats_df.to_dict("records")),
        dcc.Store(id="psc-host-details", data=host_details),
        dcc.Store(id="psc-unique-hosts-total", data=total_unique_hosts),
        dcc.Store(id="traceroutes-grouped", data={}),
        dcc.Store(id="audit-data", data=readParquetToDf(pq, 'parquet/audited_hosts.parquet').to_dict("records")),
        dcc.Store(id="last-audit"),
        dcc.Store(id="config-hosts", data=all_hosts_in_configs),
        dcc.Store(id="cric-hosts", data=all_cric_perfsonar_hosts),

        toolkits_overloaded_UI(),
        
            
        html.Div([
                html.Div(
                    id="audit-header",
                    n_clicks=0,
                    style={"cursor": "pointer", "display": "flex", "alignItems": "center", "gap": "8px"},
                    children=[
                        html.I(id="audit-caret", className="fas fa-chevron-right", style={"transition": "transform 0.2s ease"}),
                        html.H3("Hosts Audit (PWA)", className="mt-2"),
                    ]
                ),
                dbc.Collapse(
                    id={"type": "collapse", "id": "audit-collapse"},
                    is_open=False,  # hidden by default
                    children=[
                                html.Div(id="div-audit", children=[
                                    
                                    html.H4("Host auditing is performed every 24 hours. During the audit, the host undergoes several tests, based on which it is assigned a status.", className="text mt-2", style={"font-style": "italic", "color": "#A60F0F"}),
                                    dbc.Row([
                                                        
                                        dbc.Col([
                                            html.Div(id="last-audit", className="text-secondary mb-2", style={"font-size": "1.2rem"})
                                            ]),
                                        

                                        dbc.Col(
                                            html.Div(
                                                    dbc.Button("Open PWA ↗", href="https://psconfig.opensciencegrid.org/#!/configs/58f74a4df5139f0021ac29d6",
                                                            className="me-2", target="_blank", color="secondary"),
                                            ),
                                            md=4, className="d-flex align-items-center justify-content-md-end mt-2 mt-md-0"
                                        )
                                    ]),        
                                    
                                    dbc.Row([
                                        dbc.Col([
                                            html.Label("Status", className="small text-muted"),
                                            dcc.Dropdown(id="f-status", options=[], value=None, multi=True, placeholder="All")
                                        ], md=3),
                                        dbc.Col([
                                            html.Label("Netsite", className="small text-muted"),
                                            dcc.Dropdown(
                                                id="f-netsite", options=[], value=None, multi=True, placeholder="All")
                                        ], md=3),
                                        dbc.Col([
                                            html.Label("In CRIC", className="small text-muted"),
                                            dcc.Dropdown(
                                                id="f-incric",
                                                options=[{"label":"Yes","value":True}, {"label":"No","value":False}],
                                                value=None, multi=True, placeholder="All"
                                            )
                                        ], md=3),
                                        dbc.Col([
                                            html.Label("Found in ES (30d)", className="small text-muted"),
                                            dcc.Dropdown(
                                                id="f-found",
                                                options=[{"label":"Yes","value":True}, {"label":"No","value":False}],
                                                value=None, multi=True, placeholder="All"
                                            )
                                        ], md=3),
                                    ], className="mb-2"),

                                    
                                    dcc.Loading(
                                        id="loading-charts",
                                        type="default",
                                        color="#00245A",
                                        delay_show=300,
                                        children=dbc.Row([
                                            dbc.Col(dcc.Graph(id="donut-status"), md=6),
                                            dbc.Col(dcc.Graph(id="bar-status"), md=6),
                                        ], className="mb-2")
                                    ),

                                    
                                    dcc.Loading(
                                        id="loading-table",
                                        type="default",
                                        color="#00245A",
                                        delay_show=300,
                                        parent_style={"position": "relative"},
                                        children=html.Div(id="audit-table")
                                    ),
                            ]),
                    ]),
                ], className="p-1 site boxwithshadow m-3"),
        # ---- Connectivity sanity check UI ----
            html.Div([
                html.Div(
                    id="connectivity-header",
                    n_clicks=0,
                    style={"cursor": "pointer", "display": "flex", "alignItems": "center", "gap": "8px"},
                    children=[
                        html.I(id="connectivity-caret", className="fas fa-chevron-right", style={"transition": "transform 0.2s ease"}),
                        html.H3("Network Connectivity Details (T1 ↔ T1)", className="mt-2 mb-0")
                    ]
                ),
                dbc.Collapse(
                    id={"type": "collapse", "id": "connectivity-collapse"},
                    is_open=False, 
                    children=[
                        dbc.Alert(
                            [
                                html.Div("Assumption: every T1 site should have traceroute tests with every other T1 site.",
                                        className="mb-1", style={"font-size": "1.3rem", "font-style": "italic"}),
                                html.Ul([
                                    html.Li("Priority 1 — Missing connections: no test observed in this role (as source / as destination).", style={"font-size": "1.3rem", "font-style": "italic"}),
                                    html.Li("Priority 2 — All tested connections are RED in a role: likely perfSONAR toolkit misconfiguration at the site.", style={"font-size": "1.3rem", "font-style": "italic"}),
                                    html.Li("Priority 3 — YELLOW connections: destination never reached but path complete (possible firewall/ICMP filtering).", style={"font-size": "1.3rem", "font-style": "italic"}),
                                ], className="mb-0")
                            ],
                            color="light",
                            className="mb-2"
                        ),

                        
                        html.Div([
                            html.H5("Connectivity ranking", className="mt-2"),
                            dt.DataTable(
                                id="connectivity-table",
                                columns=[
                                    {"name": "Site", "id": "site"},
                                    # Source role
                                    {"name": "Expected (src)", "id": "expected_src", "type": "numeric"},
                                    {"name": "Missing (src)",  "id": "missing_src",  "type": "numeric"},
                                    {"name": "Tested (src)",   "id": "tested_src",   "type": "numeric"},
                                    {"name": "Green (src)",    "id": "green_src",    "type": "numeric"},
                                    {"name": "Yellow (src)",   "id": "yellow_src",   "type": "numeric"},
                                    {"name": "Red (src)",      "id": "red_src",      "type": "numeric"},
                                    # {"name": "All tested RED (src)", "id": "all_red_src_flag", "type": "text"},
                                    # Destination role
                                    {"name": "Expected (dest)", "id": "expected_dest", "type": "numeric"},
                                    {"name": "Missing (dest)",  "id": "missing_dest",  "type": "numeric"},
                                    {"name": "Tested (dest)",   "id": "tested_dest",   "type": "numeric"},
                                    {"name": "Green (dest)",    "id": "green_dest",    "type": "numeric"},
                                    {"name": "Yellow (dest)",   "id": "yellow_dest",   "type": "numeric"},
                                    {"name": "Red (dest)",      "id": "red_dest",      "type": "numeric"},
                                    # {"name": "All tested RED (dest)", "id": "all_red_dest_flag", "type": "text"},
                                ],
                                data=summary_df.to_dict("records"),
                                sort_action="native",
                                page_size=16,
                                style_table={"overflowX": "auto"},
                                style_cell={"fontFamily": "Inter, system-ui", "fontSize": 13, "padding": "6px"},
                                style_header={"fontWeight": "600"},
                                style_data_conditional=[
                                    # Light highlight when any missing in either role
                                        {
                                            'if': {
                                                'filter_query': '{tested_src} = 0',
                                                'column_id': 'tested_src'
                                            },
                                            'backgroundColor': '#5957579A',
                                            'color': 'white'
                                        }, 
                                        {
                                            'if': {
                                                'filter_query': '{tested_dest} = 0',
                                                'column_id': 'tested_dest'
                                            },
                                            'backgroundColor': "#5957579A",
                                            'color': 'white'
                                        },
                                        {
                                            "if": {
                                                "filter_query": "({all_red_src_flag} = 'True')  || ({red_src} > 4)",
                                                "column_id": ["site", "red_src"]
                                            },
                                            "backgroundColor": "#a0000077",
                                            "color": "white"
                                        },
                                        {
                                            "if": {
                                                "filter_query": "({all_red_dest_flag} = 'True')  || (({green_dest} < 5) && ({green_dest} < {tested_dest}))",
                                                "column_id": ["site", "red_dest"]
                                            },
                                            "backgroundColor": "#a0000077",
                                            "color": "white"
                                        },
                                    ],
                                )
                            ], className="mb-3"),

                            # ---------- Exactly which connections are missing (role-aware) ----------
                            html.Div([
                                html.H5("Missing connections", className="mt-2"),
                                dt.DataTable(
                                    id="missing-table",
                                    columns=[
                                        {"name": "Site", "id": "site"},
                                        {"name": "Role", "id": "role"},  # "as_src" or "as_dest"
                                        {"name": "Missing with (no test observed)", "id": "missing_with"},
                                    ],
                                    data=missing_df.to_dict("records"),
                                    page_size=16,
                                    style_table={"overflowX": "auto"},
                                    style_cell={"fontFamily": "Inter, system-ui", "fontSize": 13, "padding": "6px"},
                                    style_header={"fontWeight": "600"},
                                ),
                            ], className="mb-4")
                        ]
                    )
                
        ], className="p-1 site boxwithshadow m-3"),
        dcc.Location(id="url-networking", refresh=False),
        html.Div(id="div-configs", children=[
            
            html.Div(children=[
                
                dcc.Store(id="valid-data", data=traceroutes_dict),
                html.Div([
                    html.H1("T1/OPN Network Connectivity Monitor", 
                            className="mb-1", 
                            style={"color": "#2c3e50", "font-weight": "bold"}),
                    html.H3("Traceroute Test Results — Last 2 Hours", 
                        className="text-secondary", 
                        style={"font-weight": "normal"}),

                    
                    html.Div([
                    html.I(className="fas fa-clock me-2 text-muted"),
                    html.Small(f"Data period: {FROM_DATE.strftime('%Y-%m-%d %H:%M')} — {TO_DATE.strftime('%Y-%m-%d %H:%M')} UTC", className="text-muted")
                    ], className="text-center mb-3", style={"font-size": "0.9rem"}),

                ], className="text-center mt-1 mb-3"),


                                dbc.Row([
                                    dbc.Col(
                                        dcc.Dropdown(
                                            multi=True,
                                            id="t1-site-filter",
                                            placeholder="Filter T1 sites…",
                                            options= T1_NETSITES,
                                            value=T1_NETSITES
                                        ),
                                        md=8, xl=10
                                    ),
                                    dbc.Col(
                                        html.Div([
                                            
                                            html.Div([
                                                dbc.Button("Search", id="btn-search", className="me-2 mb-1", n_clicks=0),
                                                dbc.Button("Select all", id="btn-select-all", className="me-2 mb-1", n_clicks=0),
                                                dbc.Button("Clear", id="btn-clear", color="secondary", n_clicks=0),
                                            ], className="d-inline-block"),
                                        ], className="d-flex flex-column align-items-end justify-content-md-end mt-2 mt-md-0")
                                    )
                                ], className="mb-1"),

                                
                                dbc.Row(
                                    dbc.Col(
                                        dcc.Loading(
                                            id="loading-map",
                                            type="default",
                                            color='#00245A',
                                            children=[
                                                    dcc.Graph(id='traceroute-map', figure=buildMap(sites_status[sites_status['site'].isin(T1_NETSITES)], True, grouped),
                                                            style={'height': '75vh'})
                                                    ]
                                        ),
                                        width=12
                                        )
                                ),

                                dcc.Store(id="time-window", data={
                                    "from": FROM_DATE.strftime(hp.DATE_FORMAT), "to": TO_DATE.strftime(hp.DATE_FORMAT), "hours": 2
                                }),
                                dcc.Store(id="t1-selected", data=T1_NETSITES)
                            
            ], className="p-1 site boxwithshadow page-cont m-3"),
            dcc.Store(id="connectivity-summary-initial", data=summary_df.to_dict("records")),
            dcc.Store(id="missing-summary-initial", data=missing_df.to_dict("records")),
        ]),    
            
 ])

def explode_psconfig(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Groups_cnt"] = df["Groups"].apply(len)
    df["Types_cnt"]  = df["Types"].apply(len)
    df = df[df["Groups_cnt"] == df["Types_cnt"]].copy()
    df = df.explode(['Groups', 'Types', 'Schedules'])
    default_sched = {"repeat": None, "slip": None}
    df["Schedules"] = df["Schedules"].apply(lambda x: default_sched if pd.isna(x) else x)
    df["Types"] = df["Types"].astype(str).str.lower()
    df = df.drop_duplicates(subset=['Host', 'Types', 'Groups'])
    return df




def _build_group_maps(df_exploded: pd.DataFrame):
    """
    Build lookups equivalent to:
      df = data[data['Groups'] == g]
      t = df['Types'].unique()[0]
      size = len(df)
      sched = df['Schedules'].tolist()[0]
    """
    grp_type = (df_exploded.groupby('Groups')['Types']
                .agg(lambda s: s.iloc[0])
                .to_dict())

    grp_size = df_exploded.groupby('Groups').size().to_dict()

    def first_sched(s):
        x = s.iloc[0]
        return x if isinstance(x, dict) else {"repeat": None, "slip": None}
    grp_sched = (df_exploded.groupby('Groups')['Schedules']
                 .agg(first_sched)
                 .to_dict())

    host_groups = (df_exploded.groupby('Host')['Groups']
                   .agg(lambda s: list(s))
                   .to_dict())

    return grp_type, grp_size, grp_sched, host_groups


def host_stats(host: str,
               grp_type: dict,
               grp_size: dict,
               grp_sched: dict,
               host_groups: dict):
    """
    EXACT semantics of your original loop:
      - iterate test_meshes a host belongs to
      - per group g: type = unique()[0], size = len(df), schedule = first row
      - tests_num += size
      - tests_num_per_type_total[type] += (size - 1)
      - tests_num -= len(test_meshes)
    Returns:
      tests_num, throughput, traceroutes, latency, latencybg, rtt, tests_num_per_type_detail
    """
    test_meshes = host_groups.get(host, [])
    
    tests_num_per_type = {
        'throughput': [], 'trace': [], 'latencybg': [], 'latency': [], 'rtt': []
    }
    tests_num_per_type_total = {
        'throughput': 0, 'trace': 0, 'latencybg': 0, 'latency': 0, 'rtt': 0
    }
    tests_num = 0

    for g in test_meshes:
        t = grp_type.get(g)              
        size = int(grp_size.get(g, 0))    
        sched = grp_sched.get(g, {"repeat": None, "slip": None}) 
        if host == "ccperfsonar1.in2p3.fr" and g == "WLCG ATLAS Traceroute IPv6":
            print("grp_size")
            print(grp_size)

        if size > 0 and t in tests_num_per_type:
            tests_num_per_type[t].append((g, size, sched.get('repeat'), sched.get('slip')))
            tests_num += size
            tests_num_per_type_total[t] += (size - 1)
            

    tests_num -= len(test_meshes)

    tgpt_tests_num = tests_num_per_type_total['throughput']
    trc_test_num   = tests_num_per_type_total['trace']
    ltcy_test_num  = tests_num_per_type_total['latency']
    ltcybg_test_num= tests_num_per_type_total['latencybg']
    rtt_test_num   = tests_num_per_type_total['rtt']
    if host == "ccperfsonar1.in2p3.fr":
                print(host)
                print(test_meshes)
                print(tests_num_per_type)
                print(tests_num_per_type_total)
                print(tests_num)

    return tests_num, tgpt_tests_num, trc_test_num, ltcy_test_num, ltcybg_test_num, rtt_test_num, tests_num_per_type

def build_psc_stats_table(df_exploded: pd.DataFrame):
    grp_type, grp_size, grp_sched, host_groups = _build_group_maps(df_exploded)

    rows = []
    host_details = {} 
    for h in sorted(host_groups.keys()):
        total_num, th, tr, lat, latbg, rtt, other_stats = host_stats(
            h, grp_type, grp_size, grp_sched, host_groups
        )
        rows.append([h, total_num, th, tr, lat, latbg, rtt])
        host_details[h] = other_stats

    stats_df = pd.DataFrame(rows, columns=['Host', "Total", "Throughput", "Trace", "Latency", "Latencybg", "Rtt"])
    stats_df = stats_df.sort_values(by=['Total'], ascending=False)
    return stats_df, host_details

@dash.callback(
    Output("psc-selected-host", "data"),
    Output("psc-modal", "is_open"),
    Output("psc-modal-title", "children"),
    Output("psc-modal-body", "children"),
    Input("psc-table", "active_cell"),
    Input("psc-modal-close", "n_clicks"),
    State("psc-table", "data"),
    State("psc-host-details", "data"),
    State("psc-modal", "is_open"),
    prevent_initial_call=True
)
def toggle_host_modal(active_cell, close_clicks, table_data, details_map, is_open):
    if close_clicks and (dash.callback_context.triggered and
                         dash.callback_context.triggered[0]["prop_id"].startswith("psc-modal-close")):
        return dash.no_update, False, dash.no_update, dash.no_update

    if not active_cell or not table_data:
        raise PreventUpdate

    row = active_cell.get("row")
    col_id = active_cell.get("column_id")
    if row is None or row < 0 or row >= len(table_data):
        raise PreventUpdate

    if col_id != "Host":
        raise PreventUpdate

    raw_host = table_data[row].get("Host", "")
    if isinstance(raw_host, str) and raw_host.startswith("[") and "](" in raw_host:
        host = raw_host[1:raw_host.index("]")]
    else:
        host = raw_host

    host_detail = (details_map or {}).get(host)

    if not host_detail:
        body = html.Div("No details found for this host.")
        return host, True, f"Groups for {host}", body

    order = ["throughput", "trace", "latency", "latencybg", "rtt"]
    pretty = {
        "throughput": "Throughput",
        "trace": "Traceroute",
        "latency": "Latency",
        "latencybg": "Latencybg",
        "rtt": "RTT",
    }

    sections = []
    for t in order:
        rows = host_detail.get(t, [])
        if not rows:
            continue
        # rows: (group, size, repeat, slip)
        df = pd.DataFrame(rows, columns=["Group", "Hosts in Group", "repeat", "slip"])
        tbl = dt.DataTable(
            data=df.to_dict("records"),
            columns=[{"name": c, "id": c} for c in df.columns],
            page_size=min(12, len(df)),
            style_table={"overflowX": "auto"},
            style_cell={"fontFamily": "Inter, system-ui", "fontSize": 13, "padding": "6px"},
            style_header={"fontWeight": "600"},
        )
        sections.append(
            dbc.AccordionItem(
                title=f"{pretty[t]} — {len(rows)} groups",
                children=tbl
            )
        )

    body = dbc.Accordion(children=sections, start_collapsed=True, always_open=False) if sections else html.Div(
        "No groups registered for this host."
    )
    return host, True, f"Groups for {host}", body


@dash.callback(
    Output("url-configs", "hash", allow_duplicate=True),
    Input({"type": "collapse", "id": ALL}, "value"),
    prevent_initial_call=True,
)
def update_hash(value):
    """Update the hash in the URL Location component to represent the app state.

    The app state is json serialised then base64 encoded and is treated with the
    reverse process in the layout function.
    """
    print("IN update_hash...")
    return "#" + value
    
    
def toggle(n, is_open, hash=""):
    open_now = not is_open if n else is_open
    style = {"transition": "transform 0.2s ease", "transform": "rotate(90deg)" if open_now else "rotate(0deg)"}
    return open_now, style
    

@dash.callback(
    Output({"type": "collapse", "id": "connectivity-collapse"}, "is_open"),
    Output("connectivity-caret", "style"),
    Input("connectivity-header", "n_clicks"),
    State({"type": "collapse", "id": "connectivity-collapse"}, "is_open"),
)
def toggle_connectivity(n, is_open):
    return toggle(n, is_open)

@dash.callback(
    Output({"type": "collapse", "id": "audit-collapse"}, "is_open"),
    Output("audit-caret", "style"),
    Input("audit-header", "n_clicks"),
    State({"type": "collapse", "id": "audit-collapse"}, "is_open"),
)
def toggle_audit(n, is_open):
    return toggle(n, is_open)

@dash.callback(
    Output({"type": "collapse", "id": "toolkits-overloaded-collapse"}, "is_open"),
    Output("toolkits-overloaded-caret", "style"),
    Input("toolkits-overloaded-header", "n_clicks"),
    State({"type": "collapse", "id": "toolkits-overloaded-collapse"}, "is_open"),
)
def toggle_toolkit_overloaded(n, is_open):
    return toggle(n, is_open)

@dash.callback(
    Output("psc-bar-top", "figure"),
    Output("psc-table-wrap", "children"),
    Output("psc-filter-netsite", "options"),
    Output("psc-filter-host", "options"),
    Output("psc-unique-hosts-line", "children"), 
    Input("psc-top-n", "value"),
    Input("psc-cols", "value"),
    Input("psc-filter-netsite", "value"),
    Input("psc-filter-host", "value"),
    State("psc-exploded", "data"),
    State("psc-host-stats", "data"),
    State("psc-unique-hosts-total", "data"),
    State("audit-data", "data"),
    prevent_initial_call=False
)
def render_psc_host_load(top_n, show_cols, selected_netsites, selected_hosts, mesh_records, stats_records, total_unique_hosts, audit_records):
    stats = pd.DataFrame(stats_records or [])
    audit = pd.DataFrame(audit_records or [])
    
    netsite_opts = []
    if not audit.empty:
        if "netsite" in audit.columns:
            netsites = sorted(audit["netsite"].dropna().astype(str).unique().tolist())
            netsite_opts = [{"label": s, "value": s} for s in netsites]
        if "host" in audit.columns:
            hosts = sorted(audit["host"].dropna().astype(str).unique().tolist())
            host_opts = [{"label": h, "value": h} for h in hosts]
        

    if stats.empty:
        empty_fig = px.bar(title="No psConfig data")
        table = dt.DataTable(data=[], columns=[])
        badge = html.Div([
                    dbc.Badge(f"Unique hosts in psConfig: 0", color="secondary", className="me-2 size-sm"),
                ])
        return empty_fig, table, netsite_opts, host_opts, badge

    host_to_site = {}
    if not audit.empty and {"host","netsite"}.issubset(audit.columns):
        host_to_site = {
            str(row["host"]).strip().lower(): (None if pd.isna(row["netsite"]) else str(row["netsite"]))
            for _, row in audit.iterrows()
        }

    if selected_netsites:
        def belongs_to_selected(h):
            site = host_to_site.get(str(h).strip().lower())
            return site in set(selected_netsites)
        stats = stats[stats["Host"].map(belongs_to_selected)]
    
    if selected_hosts:
        stats = stats[stats["Host"].isin(selected_hosts)]

    badges = html.Div([
        dbc.Badge(f"Unique hosts in psConfig: {total_unique_hosts}", color="secondary", className="me-2 size-sm"),
    ])

    show_cols = [c for c in (show_cols or []) if c in ['Throughput',"Trace","Latency","Latencybg",'Rtt']]
    display_cols = ['Host','Total'] + show_cols

    if top_n is None or top_n < 1:
        top_n = 20
    stats = stats.sort_values('Total', ascending=False)
    top_df = stats.head(int(top_n)).copy()

    if show_cols:
        m = (top_df[['Host'] + show_cols]
             .melt(id_vars='Host', var_name='Type', value_name='Count'))
        fig = px.bar(m, x='Host', y='Count', color='Type',
                     title=f"Top {top_n} hosts by psConfig test count",
                     barmode='group')
        fig.update_layout(xaxis={'tickangle': -35})
    else:
        fig = px.bar(top_df, x='Host', y='Total',
                     title=f"Top {top_n} hosts by psConfig test count")
        fig.update_layout(xaxis={'tickangle': -35})
    

    disp = stats[display_cols].copy()
    disp["Host"] = disp["Host"].apply(lambda h: f"[{h}](#)") #it makes hosts look clickable, but it also works as link - that is not the best practice, but I haven't found another approach

    table = dt.DataTable(
        id="psc-table",
        data=disp.to_dict('records'),
        columns=[
            {"name": "Host", "id": "Host", "presentation": "markdown"},
            *[{"name": c, "id": c} for c in display_cols if c != "Host"]
        ],
        markdown_options={"html": True, "link_target": "_self"},
        sort_action="native",
        filter_action="native",
        page_size=20,
        tooltip_data=[ 
            {"Host": {"value": "Click to see groups & schedules"}} for _ in range(len(disp))
        ],
        tooltip_duration=None,
        style_table={"overflowX": "auto"},
        style_cell={"fontFamily": "Inter, system-ui", "fontSize": 13, "padding": "6px"},
        style_header={"fontWeight": "600"},
        style_data_conditional=[
            # thresholds to color table background
            {"if": {"filter_query": "{Total} >= 100"}, "backgroundColor": "rgba(217,48,37,0.08)"},
            {"if": {"filter_query": "{Total} >= 50 && {Total} < 100"}, "backgroundColor": "rgba(255,193,7,0.10)"},
        ],
        # CSS selectors give pointer cursor + blue underlined link just for the Host column
        css=[
            {"selector": ".dash-table-container .dash-cell.column-Host", "rule": "cursor: pointer;"},
            {"selector": ".dash-table-container .dash-cell.column-Host a", "rule": "color:#0d6efd; text-decoration: underline;"},
            {"selector": ".dash-table-container .dash-cell.column-Host a:focus", "rule": "outline: 2px solid #0d6efd33; outline-offset: 2px;"},
        ],
    )

    return fig, table, netsite_opts, host_opts, badges

STATUS_PRETTY = {
    "ACTIVE_HTTP": "Active HTTP — reachable; toolkit/API responds",
    "ACTIVE_TCP_ONLY": "Active TCP only — host up; web check fails; some TCP ports respond",
    "UNREACHABLE_CANDIDATE": "Unreachable — DNS OK; nothing answers; likely down/firewalled",
    "RETIRED_DNS": "Retired DNS — DNS missing/invalid; entry is stale",
}


STATUS_COLORS = {
    "Active HTTP — reachable; toolkit/API responds":      "#0B8043",  
    "Active TCP only — host up; web check fails; some TCP ports respond":  "#34A853",  
    "Unreachable — DNS OK; nothing answers; likely down/firewalled":      "#A8D5B8", 
    "Retired DNS — DNS missing/invalid; entry is stale":      "#D93025",  
    "Other":            "#9E9E9E", 
}

@dash.callback(
    Output("audit-table", "children"),
    Output("donut-status", "figure"),
    Output("bar-status", "figure"),
    Output("f-status", "options"),
    Output("f-netsite", "options"),
    Output("last-audit", "children"),
    Output("audit-data", "data"),


    Input("f-status", "value"),
    Input("f-incric", "value"),
    Input("f-found", "value"),
    Input("f-netsite", "value"),
    
    State("audit-data", "data"),
    State("last-audit", "children"),
    State("config-hosts", "data"),
    State("cric-hosts", "data"),
    prevent_initial_call=False
)
def render_audit(f_status, f_incric, f_found, f_netsite, data, last_modified, config_hosts, cric_hosts):

    print("In get audit...")
    try:
        pq = Parquet()
        df = readParquetToDf(pq, 'parquet/audited_hosts.parquet')
        last_modified = datetime.fromtimestamp(Path("parquet/audited_hosts.parquet").stat().st_mtime, tz=timezone.utc)
        data, last_modified = df.to_dict("records"), f"Using cached audit from {last_modified:%Y-%m-%d %H:%M:%S} UTC"
    except Exception as e:
        print(e)
        print("Something went wrong in callback while trying to get audit parquet...")
        
    
    if not data:
        empty_fig = px.scatter(title="No data found from Audit")
        return html.Div(dt.DataTable(data=[], columns=[])), empty_fig, empty_fig, [], [], "No found", {}

    df = pd.DataFrame(data).copy()


    for c in ["status", "in_cric", "found_in_ES"]:
        if c not in df.columns:
            df[c] = np.nan
    df["in_cric"] = df["host"].isin(cric_hosts)
    if df["in_cric"].dtype != bool:
        df["in_cric"] = df["in_cric"].astype(bool, errors="ignore")
    if df["found_in_ES"].dtype != bool:
        df["found_in_ES"] = df["found_in_ES"].astype(bool, errors="ignore")

    all_statuses = sorted(df["status"].dropna().astype(str).unique().tolist())
    status_opts = [{"label": s, "value": s} for s in all_statuses]
    
    all_netsites = sorted(df["netsite"].dropna().astype(str).unique().tolist())
    netstite_opts = [{"label": s, "value": s} for s in all_netsites]

    # filters
    fdf = df.copy()
    if f_status:
        fdf = fdf[fdf["status"].astype(str).isin(f_status)]
    if f_incric:
        fdf = fdf[fdf["in_cric"].isin(f_incric)]
    if f_found:
        fdf = fdf[fdf["found_in_ES"].isin(f_found)]
    if f_netsite:
        fdf = fdf[fdf["netsite"].astype(str).isin(f_netsite)]

    # ---------- Table ----------
    tdf = fdf.copy()
    tdf["in_cric"] = np.where(tdf["in_cric"], "Yes", "No")
    tdf["found_in_ES"] = np.where(tdf["found_in_ES"], "Yes", "No")
    display_cols = [c for c in ["host","netsite","status","in_cric","found_in_ES","suggestion"] if c in tdf.columns]
    table = dt.DataTable(
        data=tdf[display_cols].to_dict("records"),
        columns=[{"name": c.replace("_"," ").title(), "id": c} for c in display_cols],
        sort_action="native",
        filter_action="native",
        page_size=20,
        style_table={"overflowX": "auto"},
        style_cell={"fontFamily": "Inter, system-ui", "fontSize": 13, "padding": "6px"},
        style_header={"fontWeight": "600"},
        style_data_conditional=[
            {"if": {"filter_query": "{suggestion} = 'Delete from psConfig & CRIC'"},
             "backgroundColor": "rgba(255,0,0,0.08)"},
            {"if": {"filter_query": "{suggestion} = 'Delete from psConfig'"},
             "backgroundColor": "rgba(255,128,0,0.08)"},
            {"if": {"filter_query": "{suggestion} = 'Add to CRIC'"},
             "backgroundColor": "rgba(0,128,255,0.08)"},
            {"if": {"filter_query": "{suggestion} contains 'Investigate'"},
             "backgroundColor": "rgba(255,215,0,0.10)"},
        ],
    )

  
    gdf = fdf.copy()
    gdf["status_group"] = (
        gdf["status"].map(STATUS_PRETTY).fillna("Other")
    )

    donut_counts = gdf.groupby("status_group").size().reset_index(name="count")
    donut_counts["color_key"] = donut_counts["status_group"].map(lambda s: STATUS_COLORS.get(s, STATUS_COLORS["Other"]))
    fig_donut = px.pie(
        donut_counts, names="status_group", values="count",
        hole=0.45, title="Hosts by Status"
    )

    fig_donut.update_traces(marker=dict(colors=[STATUS_COLORS.get(s, STATUS_COLORS["Other"])
                                                for s in donut_counts["status_group"]]),
                            textposition="inside")
    fig_donut.update_layout(showlegend=True, annotations=[dict(text=len(tdf), font_size=14, showarrow=False, xanchor="center")],
                            legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=-0.35,
                                    xanchor="center",
                                    x=0.5,
                                    maxheight=0.3, 
                                    title_text="Status")
                            )

    bar_incric = (
        gdf.groupby(["status","in_cric"]).size().reset_index(name="count")
        .assign(metric="In CRIC",
                flag=lambda d: d["in_cric"].map({True:"Yes", False:"No"}))
        .drop(columns=["in_cric"])
    )
    bar_found = (
        gdf.groupby(["status","found_in_ES"]).size().reset_index(name="count")
        .assign(metric="Found in ES",
                flag=lambda d: d["found_in_ES"].map({True:"Yes", False:"No"}))
        .drop(columns=["found_in_ES"])
    )
    bar_df = pd.concat([bar_incric, bar_found], ignore_index=True)
    bar_df["series"] = bar_df["metric"] + ": " + bar_df["flag"].astype(str)
    print(bar_df["series"].unique())
    palette = {
        "In CRIC: Yes":  "#98df8a",
        "In CRIC: No": "#D46057",
        "Found in ES: Yes":    "#34A853",
        "Found in ES: No":   "#D93025",
    }
    order = list(palette.keys())
    fig_bar = px.bar(
        bar_df,
        x="status",
        y="count",
        color="series",
        barmode="group",
        title="Host Found In CRIC & ES",
        category_orders={"series": order},
        color_discrete_map=palette
    )
    fig_bar.update_layout(xaxis={'categoryorder': 'total descending'})

    return table, fig_donut, fig_bar, status_opts, netstite_opts, last_modified, data


@dash.callback(
    [
        Output("traceroute-map", "figure"),
        Output("t1-selected", "data"),
        Output("t1-site-filter", "value"),
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


    button = ctx.split('.')[0]


    pq = Parquet()
    alarm_cnt = pq.readFile('parquet/alarmsGrouped.parquet')
    _, sites_status = generateStatusTable(alarm_cnt)
    sites_status = sites_status[sites_status['site'].isin(T1_NETSITES)]

    if button == 'btn-clear':
        return (
            buildMap(sites_status),
            T1_NETSITES,
            None,
        )

    elif button == 'btn-select-all':
        selected_sites = T1_NETSITES

    df = pd.DataFrame(df)

    if selected_sites:
        # print('df')
        # print(df)
        df = df[df["src_netsite"].isin(selected_sites) | df["dest_netsite"].isin(selected_sites)]
        # print('df')
        # print(df)

    if df.empty or not selected_sites:
        empty_grouped = pd.DataFrame(columns=[
            'pair','src_netsite','dest_netsite','src_host','dest_host','path_complete',
            'destination_reached','path_complete_stats','destination_reached_stats', 'color'
        ])
        return (
            buildMap(sites_status, True, empty_grouped),
            T1_NETSITES,
            selected_sites
        )
        
    # print(summary_df)
    return (
        buildMap(sites_status, True, df),
        list(set(T1_NETSITES) - set(selected_sites)),
        selected_sites
    )


SEVERITY_ORDER = {"red": 3, "yellow": 2, "green": 1}

def _pair_status(row) -> str:
    """
    Classify a directed pair row (src → dst).
    - green  : destination_reached == True
    - yellow : destination_reached == False and path_complete == True  (firewall / filtered ICMP likely)
    - red    : otherwise (destination not reached & path incomplete, or anything else)
    """
    dr = bool(row.get("destination_reached", False))
    pc = bool(row.get("path_complete", False))
    if dr:
        return "green"
    if (not dr) and pc:
        return "yellow"
    return "red"


import pandas as pd

def compute_connectivity_summaries(grouped_df: pd.DataFrame, sites: list[str]):
    """
    Inputs:
      grouped_df: dataframe (one row per directed pair observation) with columns:
                  'src_netsite','dest_netsite','destination_reached','path_complete','color'
      sites:      list of T1 netsites (T1_NETSITES)

    Returns:
      summary_df: per-site ranking with columns:
                  site,
                  expected_src, missing_src, tested_src, green_src, yellow_src, red_src, all_red_src_flag,
                  expected_dest, missing_dest, tested_dest, green_dest, yellow_dest, red_dest, all_red_dest_flag
      missing_expanded_df: rows describing omissions:
                  site, role ('as_src' | 'as_dest'), missing_with (comma-separated peers)
    """
    print("in compute_connectivity_summaries_directed...")
    # print(grouped_df)
    # print(sites)

    # Fallback severity if not provided globally: lower = worse
    sev_default = {'red': 0, 'yellow': 1, 'green': 2}
    SEVERITY = globals().get('SEVERITY_ORDER', sev_default)

    if grouped_df is None or grouped_df.empty:
        total_possible = max(len(sites) - 1, 0)
        summary_df = pd.DataFrame([{
            "site": s,
            "expected_src": total_possible, "missing_src": total_possible, "tested_src": 0,
            "green_src": 0, "yellow_src": 0, "red_src": 0, "all_red_src_flag": False,
            "expected_dest": total_possible, "missing_dest": total_possible, "tested_dest": 0,
            "green_dest": 0, "yellow_dest": 0, "red_dest": 0, "all_red_dest_flag": False,
        } for s in sites]).sort_values(["missing_src","missing_dest"], ascending=False, kind="stable")

        missing_rows = []
        for s in sites:
            peers = ", ".join(sorted([x for x in sites if x != s]))
            if peers:
                missing_rows.append({"site": s, "role": "as_src",  "missing_with": peers})
                missing_rows.append({"site": s, "role": "as_dest", "missing_with": peers})
        missing_expanded_df = pd.DataFrame(missing_rows)
        return summary_df, missing_expanded_df


    directed = grouped_df[["src_netsite","dest_netsite","color"]].dropna(subset=["src_netsite","dest_netsite"]).copy()


    worst_directed = {}
    for _, r in directed.iterrows():
        a, b, st = r["src_netsite"], r["dest_netsite"], r["color"]
        if a == b:
            continue
        key = (a, b)
        if key not in worst_directed:
            worst_directed[key] = st
        else:
            if SEVERITY.get(st, 99) < SEVERITY.get(worst_directed[key], 99):
                worst_directed[key] = st

    rows = []
    missing_rows = []
    for s in sites:
        others = [x for x in sites if x != s]
        expected_src = len(others)
        expected_dest = len(others)


        green_src = yellow_src = red_src = 0
        missing_src_with = []
        for o in others:
            key = (s, o)
            if key not in worst_directed:
                missing_src_with.append(o)
            else:
                st = worst_directed[key]
                if st == "green":
                    green_src += 1
                elif st == "yellow":
                    yellow_src += 1
                else:
                    red_src += 1
        missing_src = len(missing_src_with)
        tested_src = expected_src - missing_src
        all_red_src_flag = (tested_src > 0 and green_src == 0 and yellow_src == 0 and red_src == tested_src)


        green_dest = yellow_dest = red_dest = 0
        missing_dest_with = []
        for o in others:
            key = (o, s)
            if key not in worst_directed:
                missing_dest_with.append(o)
            else:
                st = worst_directed[key]
                if st == "green":
                    green_dest += 1
                elif st == "yellow":
                    yellow_dest += 1
                else:
                    red_dest += 1
        missing_dest = len(missing_dest_with)
        tested_dest = expected_dest - missing_dest
        all_red_dest_flag = (tested_dest > 0 and green_dest == 0 and yellow_dest == 0 and red_dest == tested_dest)

        if missing_src_with:
            missing_rows.append({"site": s, "role": "as_src", "missing_with": ", ".join(sorted(missing_src_with))})
        if missing_dest_with:
            missing_rows.append({"site": s, "role": "as_dest", "missing_with": ", ".join(sorted(missing_dest_with))})

        rows.append({
            "site": s,
            "expected_src": expected_src, "missing_src": missing_src, "tested_src": tested_src,
            "green_src": green_src, "yellow_src": yellow_src, "red_src": red_src,
            "all_red_src_flag": all_red_src_flag,
            "expected_dest": expected_dest, "missing_dest": missing_dest, "tested_dest": tested_dest,
            "green_dest": green_dest, "yellow_dest": yellow_dest, "red_dest": red_dest,
            "all_red_dest_flag": all_red_dest_flag,
        })

    summary_df = pd.DataFrame(rows)

    summary_df["all_red_src_sort"]  = summary_df["all_red_src_flag"].astype(int)
    summary_df["all_red_dest_sort"] = summary_df["all_red_dest_flag"].astype(int)
    summary_df["yellow_total"] = summary_df["yellow_src"] + summary_df["yellow_dest"]
    summary_df["red_total"]    = summary_df["red_src"] + summary_df["red_dest"]

    summary_df = summary_df.sort_values(
        by=["missing_src", "missing_dest", "all_red_src_sort", "all_red_dest_sort", "yellow_total", "red_total"],
        ascending=[False, False, False, False, False, False],
        kind="stable"
    ).drop(columns=["all_red_src_sort","all_red_dest_sort","yellow_total","red_total"])

    missing_expanded_df = pd.DataFrame(missing_rows).sort_values(["site","role"], kind="stable")
    summary_df["all_red_src_flag"]  = summary_df["all_red_src_flag"].astype(str)
    summary_df["all_red_dest_flag"] = summary_df["all_red_dest_flag"].astype(str)

    return summary_df, missing_expanded_df
