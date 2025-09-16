#DONE: change bows (up and down) DONE
#DONE: add title that this are T1 sites last 2 hours
#DONE: increase map, buttons for increasing the map
#DONE: initial loading
#DONE: change loading widget

#TODO: aggregation of records(I'm not aggregating, the query is bad but represents what I want to do)
#TODO: make a switch between latency, traceroute, packetloss?
#TODO: visualise meshes of testing 
#TODO: query once per 2 hours, don't query every time the picture is updated

import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import pandas as pd
from datetime import datetime, timedelta
from pages.cric_extract_OPN import query_valid_trace_data

import utils.helpers as hp
from utils.parquet import Parquet
from utils.hosts_audit import audit
from utils.utils import buildMap, generateStatusTable, get_color
from dash import dash_table as dt
import numpy as np
import plotly.express as px

import urllib3
import asyncio, socket, ssl
from contextlib import suppress
import aiohttp
import model.queries as qrs
from pathlib import Path
from datetime import datetime, timezone, timedelta
from itertools import combinations

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

dash.register_page(
    __name__,
    path_template="/network-testing",
    title='Network testing',
    description="",
)
AUDIT_PARQUET = Path("parquet/audited_hosts.parquet")
AUDIT_TTL = timedelta(hours=24)
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
AUDITED_HOSTS = []


def readParquetPsConfig(pq):
    """
    The function reads parquet file that is updated every 24 \
    hours with the data from psConfig about expected hosts and tests \
    results in the Elasticsearch.
    """
    parquet_path = 'parquet/raw/psConfigData.parquet'
    try: 
        print("Reading the parquet file with psConfig data...")
        df = pq.readFile(parquet_path)
        print(f"psConfigData from parquet file: {df}")
        return df
    except Exception as err:
        print(err)
        print(f"Problems with reading the file {parquet_path}")

def readParquetCRIC(pq):
    """
    The function reads parquet file that is updated every 24 \
    hours with the data about perfSONARs from CRIC.
    """
    parquet_path = 'parquet/raw/CRICData.parquet'
    try: 
        print("Reading the parquet file with CRIC data...")
        lst = pq.readFile(parquet_path)['host'].tolist()
        # print(f"CRIC data from parquet file: {lst}")
        return lst
    except Exception as err:
        print(err)
        print(f"Problems with reading the file {parquet_path}")

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
    
    print(records_df[records_df['dest_netsite'] == 'USCMS-FNAL-WC1-LHCOPNE'])
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
    grouped["src_netsite"] = grouped["src_netsite"].apply(lambda s: s.upper())
    grouped["dest_netsite"] = grouped["dest_netsite"].apply(lambda s: s.upper())
    traceroutes_dict = grouped.to_dict('records')
    # print("traceroutes_dict")
    # print(grouped[grouped['dest_netsite'] == 'USCMS-FNAL-WC1-LHCOPNE'])
    summary_df, missing_df = compute_connectivity_summaries(grouped, T1_NETSITES)
    
    
    
    
    print(" --- getting hosts from psConfigAdmin ---")
    mesh_config = readParquetPsConfig(pq)
    all_hosts_in_configs = mesh_config['Host'].unique()
    print(f"All hosts in psConfig: {len(all_hosts_in_configs)}\n")
    print(mesh_config.head(10))
    
    # configurations = extract_groups_and_hosts(mesh_config, "PIC-LHCOPNE")
    # netsites = sorted(set(mesh_config["Site"].tolist()))
    # hosts = sorted(set(mesh_config["Host"].tolist()))
    
    print(" --- getting PerfSonars from WLCG CRIC ---")
    all_cric_perfsonar_hosts = readParquetCRIC(pq)
    print(f"All perfSONAR hosts in CRIC: {len(all_cric_perfsonar_hosts)}\n")

    
    # UI
    return html.Div(children=[
        html.Div(children=[
            # Title
            dcc.Store(id="valid-data", data=traceroutes_dict),
            dcc.Store(id="config_hosts", data=all_hosts_in_configs),
            dcc.Store(id="configs", data=mesh_config.to_dict()),
            dcc.Store(id="cric_hosts", data=all_cric_perfsonar_hosts),
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

                            dcc.Store(id="time-window", data={
                                "from": date_from_str, "to": date_to_str, "hours": HOURS
                            }),
                            dcc.Store(id="t1-selected", data=T1_NETSITES)
                        ]
                )
        ], className="p-1 site boxwithshadow page-cont m-3"),
        dcc.Store(id="connectivity-summary-initial", data=summary_df.to_dict("records")),
dcc.Store(id="missing-summary-initial", data=missing_df.to_dict("records")),
        # ---- Connectivity sanity check UI ----
html.Div(children=[
    html.H3("Connectivity check (T1 ↔ T1)", className="mt-2"),
    dbc.Alert(
        [
            html.Div("Note: every T1 site should have traceroute tests with every other T1 site.", className="mb-1"),
            html.Ul([
                html.Li("Priority 1 — Missing connections: no test observed in this role (as source / as destination)."),
                html.Li("Priority 2 — All tested connections are RED in a role: likely perfSONAR toolkit misconfiguration at the site."),
                html.Li("Priority 3 — YELLOW connections: destination never reached but path complete (possible firewall/ICMP filtering)."),
            ], className="mb-0")
        ],
        color="light",
        className="mb-2"
    ),

    # ---------- Per-site combined (src + dest) ----------
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
                {"name": "All tested RED (src)", "id": "all_red_src_flag", "type": "text"},
                # Destination role
                {"name": "Expected (dest)", "id": "expected_dest", "type": "numeric"},
                {"name": "Missing (dest)",  "id": "missing_dest",  "type": "numeric"},
                {"name": "Tested (dest)",   "id": "tested_dest",   "type": "numeric"},
                {"name": "Green (dest)",    "id": "green_dest",    "type": "numeric"},
                {"name": "Yellow (dest)",   "id": "yellow_dest",   "type": "numeric"},
                {"name": "Red (dest)",      "id": "red_dest",      "type": "numeric"},
                {"name": "All tested RED (dest)", "id": "all_red_dest_flag", "type": "text"},
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
        # 🔴 Strong highlight when all connections are red (as source)
        {
            "if": {
                "filter_query": "{all_red_src_flag} = 'True'",
                "column_id": "all_red_src_flag"
            },
            "backgroundColor": "#ff000077",
            "color": "white"
        },
        # 🔴 Strong highlight when all connections are red (as destination)
        {
            'if': {
                'filter_query': "{all_red_dest_flag} = 'True'",
            },
            'backgroundColor': "#ff000077",
            'color': 'white'
        },
        # {
        #     "if": {'all_red_dest_flag': 'True'},
            
        #     "backgroundColor": "#ff000077",
        #     "color": "white"
            
        # },
    ],
)
    ], className="mb-3"),
    # html.Div(
    #     className="boxwithshadow page-cont p-3",
    #     children=[
    #         dbc.Row([
    #             dbc.Col(html.H4("Trace Coverage Matrices (last 30 days)"), md=8),
    #             dbc.Col(
    #                 dbc.Button("Clear selection", id="btn-clear-matrix", size="sm", color="secondary"),
    #                 md=4, className="d-flex justify-content-md-end align-items-center mt-2 mt-md-0"
    #             ),
    #         ], className="mb-2"),

    #         dbc.Row([
    #             dbc.Col([
    #                 html.Label("Filter by Netsite", className="fw-semibold"),
    #                 dcc.Dropdown(
    #                     id="dd-netsite",
    #                     # options=[{"label": s, "value": s} for s in netsites],
    #                     placeholder="Select a netsite…",
    #                     value=None,
    #                     clearable=True,
    #                 ),
    #             ], md=6),
    #             dbc.Col([
    #                 html.Label("Filter by Host", className="fw-semibold"),
    #                 dcc.Dropdown(
    #                     id="dd-host",
    #                     # options=[{"label": h, "value": h} for h in hosts],
    #                     placeholder="Select a host…",
    #                     value=None,
    #                     clearable=True,
    #                 ),
    #             ], md=6),
    #         ], className="mb-3"),

    #         # dcc.Store(id="store-members-by-group", data=members_by_group),
    #         # dcc.Store(id="store-host-to-site", data=host_to_site),
    #         dcc.Store(id="store-groups", data=mesh_config["Groups"]),
    #         dcc.Store(id="store-types", data=mesh_config["Types"]),

    #         dcc.Loading(
    #             id="loading-matrices",
    #             type="default",
    #             children=html.Div(id="heatmap-matrices")
    #         ),
    #     ],
    # ),

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
            data=missing_df.to_dict("records"),  # from compute_connectivity_summaries_directed(...)
            page_size=16,
            style_table={"overflowX": "auto"},
            style_cell={"fontFamily": "Inter, system-ui", "fontSize": 13, "padding": "6px"},
            style_header={"fontWeight": "600"},
        ),
    ], className="mb-4")
    ], className="p-1 site boxwithshadow page-cont m-3"),

    html.Div(children=[
        dbc.Row([dbc.Col([dbc.Row(html.H2("pSConfig Web Admin Hosts Audit", className="mt-2")),
                          html.Div(id="last-audit", className="text-secondary small mb-2")]),
                 
                #  dbc.Col(
                #         html.Div([
                #             dbc.ButtonGroup([
                #                 dbc.Button("Audit", id="audit-btn", className="me-2", n_clicks=0),
                #                 dbc.Button("Open PWA ↗", href="https://psconfig.opensciencegrid.org/#!/configs/58f74a4df5139f0021ac29d6", className="me-2", target="_blank", color="secondary"),
                #             ])
                #         ]),
                #         md=4, className="d-flex align-items-center justify-content-md-end mt-2 mt-md-0"
                #     )
                dbc.Row([
    dbc.Col(
        dcc.Loading(
            id="audit-loader",
            type="default",
            color="#00245A",
            children=html.Div(id="last-audit", className="text-secondary small mb-2")
        ),
        md=8
    ),
    dbc.Col(
        html.Div(
            dbc.ButtonGroup([
                dbc.Button("Audit", id="audit-btn", className="me-2", n_clicks=0),
                dbc.Button("Open PWA ↗", href="https://psconfig.opensciencegrid.org/#!/configs/58f74a4df5139f0021ac29d6",
                           className="me-2", target="_blank", color="secondary"),
            ])
        ),
        md=4, className="d-flex align-items-center justify-content-md-end mt-2 mt-md-0"
    ),
]),
dcc.Store(id="audit-data")
                ]),
        # dcc.Store(id="audit-data"),
        # dcc.Store(id="config-data"),
        # ---------- Filters ----------
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
        
        # ---------- Charts ----------
        dbc.Row([
            dbc.Col(
                dcc.Loading(
                    id="load-donut",
                    type="default",  # "default" | "cube" | "circle" | "dot"
                    color="#00245A",
                    children=dcc.Graph(id="donut-status")
                ),
                md=6
            ),
            dbc.Col(
                dcc.Loading(
                    id="load-bar",
                    type="default",
                    color="#00245A",
                    children=dcc.Graph(id="bar-status")
                ),
                md=6
            ),
        ], className="mb-2"),

        # ---------- Table ----------
        dbc.Row([
            dcc.Loading(
                id="loading-audit",
                type="default",
                color="#00245A",
                children=html.Div(id="audit-table")  # keep this as a single container
            )
        ]),
    ], className="p-1 site boxwithshadow page-cont m-3")
 ])
    

def _is_fresh(p: Path, ttl: timedelta) -> bool:
    if not p.exists():
        return False
    mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    return (datetime.now(timezone.utc) - mtime) < ttl

@dash.callback(
    Output("audit-data", "data"),
    Output("last-audit", "children"),
    Input("audit-btn", "n_clicks"),
    State("config_hosts", "data"),
    State("cric_hosts", "data"),
    prevent_initial_call=False
)
def run_audit(n, config_hosts, cric_hosts):
    if _is_fresh(AUDIT_PARQUET, AUDIT_TTL):
        df = pd.read_parquet(AUDIT_PARQUET)
        ts = datetime.fromtimestamp(AUDIT_PARQUET.stat().st_mtime, tz=timezone.utc)
        return df.to_dict("records"), f"Using cached audit from {ts:%Y-%m-%d %H:%M:%S} UTC"

    # Otherwise compute and cache
    audited = asyncio.run(audit(config_hosts, cric_hosts))  # <-- your coroutine
    df = pd.DataFrame(audited)
    AUDIT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(AUDIT_PARQUET, index=False)
    ts = datetime.now(timezone.utc)
    return df.to_dict("records"), f"Audited at {ts:%Y-%m-%d %H:%M:%S} UTC"

# dash.callback(
#     Output("config-data", "children"),
#     Input("audit-data", "data"),
#     Input("config-data", "data"),
#     prevent_initial_call=False
# )
# def config_data(audit_df,  confid_df):
#     # Load cache if fresh
#     if len(audit_df) < 1 or len(confid_df) < 1:
#         return "One of datasets is empty."
#     audit_df = pd.DataFrame(audit_df)
#     print("audit_df")
#     print(audit_df)
#     config_df = pd.DataFrame(config_df)
#     print("\nconfig_df")
#     print(config_df)
#     return {}

# pretty mapping for the donut

STATUS_PRETTY = {
    "ACTIVE_HTTP": "Active HTTP — reachable; toolkit/API responds",
    "ACTIVE_TCP_ONLY": "Active TCP only — host up; web check fails; some TCP ports respond",
    "UNREACHABLE_CANDIDATE": "Unreachable — DNS OK; nothing answers; likely down/firewalled",
    "RETIRED_DNS": "Retired DNS — DNS missing/invalid; entry is stale",
}

# donut colors (3 greens + red + grey for anything else)
STATUS_COLORS = {
    "Active HTTP — reachable; toolkit/API responds":      "#0B8043",  # deep green
    "Active TCP only — host up; web check fails; some TCP ports respond":  "#34A853",  # green
    "Unreachable — DNS OK; nothing answers; likely down/firewalled":      "#A8D5B8",  # pale green
    "Retired DNS — DNS missing/invalid; entry is stale":      "#D93025",  # red
    "Other":            "#9E9E9E",  # grey
}

@dash.callback(
    Output("audit-table", "children"),
    Output("donut-status", "figure"),
    Output("bar-status", "figure"),
    Output("f-status", "options"),
    Output("f-netsite", "options"),
    Input("audit-data", "data"),
    Input("f-status", "value"),
    Input("f-incric", "value"),
    Input("f-found", "value"),
    Input("f-netsite", "value"),
)
def render_audit(data, f_status, f_incric, f_found, f_netsite):
    # Empty state
    if not data:
        empty_fig = px.scatter(title="No data yet — click Audit")
        return html.Div(dt.DataTable(data=[], columns=[])), empty_fig, empty_fig, [], []

    df = pd.DataFrame(data).copy()

    # Ensure expected columns exist/types
    for c in ["status", "in_cric", "found_in_ES"]:
        if c not in df.columns:
            df[c] = np.nan
    # Normalize boolean-
    if df["in_cric"].dtype != bool:
        df["in_cric"] = df["in_cric"].astype(bool, errors="ignore")
    if df["found_in_ES"].dtype != bool:
        df["found_in_ES"] = df["found_in_ES"].astype(bool, errors="ignore")

    # Build status options dynamically
    all_statuses = sorted(df["status"].dropna().astype(str).unique().tolist())
    status_opts = [{"label": s, "value": s} for s in all_statuses]
    
    all_netsites = sorted(df["netsite"].dropna().astype(str).unique().tolist())
    netstite_opts = [{"label": s, "value": s} for s in all_netsites]

    # Apply filters (None => no filtering)
    fdf = df.copy()
    if f_status:
        fdf = fdf[fdf["status"].astype(str).isin(f_status)]
    if f_incric:
        fdf = fdf[fdf["in_cric"].isin(f_incric)]
    if f_found:
        fdf = fdf[fdf["found_in_ES"].isin(f_found)]
    if f_netsite:
        fdf = fdf[fdf["netsite"].astype(str).isin(f_netsite)]

    # ---------- Table (reacts to filters) ----------
    tdf = fdf.copy()
    # display-friendly booleans
    tdf["in_cric"] = np.where(tdf["in_cric"], "Yes", "No")
    tdf["found_in_ES"] = np.where(tdf["found_in_ES"], "Yes", "No")
    # keep a consistent column order if available
    display_cols = [c for c in ["host","netsite","status","in_cric","found_in_ES","suggestion"] if c in tdf.columns]
    table = dt.DataTable(
        data=tdf[display_cols].to_dict("records"),
        columns=[{"name": c.replace("_"," ").title(), "id": c} for c in display_cols],
        sort_action="native",
        filter_action="native",  # you still get native table filtering
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

    # ---------- Donut (reacts to filters) ----------
    gdf = fdf.copy()
    # map to pretty groups; unknowns -> "Other"
    gdf["status_group"] = (
        gdf["status"].map(STATUS_PRETTY).fillna("Other")
    )

    donut_counts = gdf.groupby("status_group").size().reset_index(name="count")
    # ensure consistent color order
    donut_counts["color_key"] = donut_counts["status_group"].map(lambda s: STATUS_COLORS.get(s, STATUS_COLORS["Other"]))
    fig_donut = px.pie(
        donut_counts, names="status_group", values="count",
        hole=0.55, title="Hosts by Status"
    )
    # apply custom colors
    fig_donut.update_traces(marker=dict(colors=[STATUS_COLORS.get(s, STATUS_COLORS["Other"])
                                                for s in donut_counts["status_group"]]),
                            textposition="inside")
    fig_donut.update_layout(showlegend=True)

    # ---------- Grouped Bar (reacts to filters) ----------
    # Build tidy counts for two metrics: In CRIC and Found in ES
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

    return table, fig_donut, fig_bar, status_opts, netstite_opts




@dash.callback(
    [
        Output("traceroute-map", "figure"),
        Output("t1-selected", "data"),
        Output("t1-site-filter", "value"),
        Output("connectivity-table", "data"),
        Output("missing-table", "data"),
    ],
    [
        Input("btn-search", "n_clicks"),
        Input("btn-select-all", "n_clicks"),
        Input("btn-clear", "n_clicks"),
        Input("t1-site-filter", "value")
    ],
    State("time-window", "data"),
    State("valid-data", "data"),
    State("connectivity-summary-initial", "data"),
    State("missing-summary-initial", "data"),
    prevent_initial_call=True
)

def update_map(search_btn, select_all_btn, clear_btn, selected_sites, tw, df, summary_df, missing_df):
    def unique_host_list(series):
        """Flatten any list-like cells, drop NAs, and return sorted unique strings."""
        vals = []
        for v in series.dropna():
            if isinstance(v, (list, tuple, set)):
                vals.extend([str(x) for x in v])
            else:
                vals.append(str(v))
        return sorted(set(vals))
    print("UPDATING MAP...")
    ctx = dash.callback_context.triggered[0]['prop_id']
    print("selected_sites")
    print(selected_sites)
    print("dash.callback_context.triggered")
    print(ctx)


    button = ctx.split('.')[0]
    print("button")
    print(button)
    
    date_from_str, date_to_str = tw["from"], tw["to"]

    pq = Parquet()
    alarm_cnt = pq.readFile('parquet/alarmsGrouped.parquet')
    _, sites_status = generateStatusTable(alarm_cnt)

    if button == 'btn-clear':
        summary_df, missing_df = compute_connectivity_summaries(
            pd.DataFrame(columns=[
                'pair','src_netsite','dest_netsite','src_host','dest_host',
                'path_complete','destination_reached','path_complete_stats','destination_reached_stats', 'color'
            ]),
            T1_NETSITES
        )
        return (
            buildMap(sites_status),
            T1_NETSITES,
            None,
            summary_df.to_dict("records"),
            missing_df.to_dict("records")
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
        summary_df, missing_df = compute_connectivity_summaries(empty_grouped, T1_NETSITES)
        return (
            buildMap(sites_status, True, empty_grouped),
            T1_NETSITES,
            selected_sites,
            summary_df.to_dict("records"),
            missing_df.to_dict("records")
        )


    summary_df, missing_df = pd.DataFrame(summary_df), pd.DataFrame(missing_df)
    
    print("in callback sumarry_df: ")
    # print(summary_df)
    return (
        buildMap(sites_status, True, df),
        list(set(T1_NETSITES) - set(selected_sites)),
        selected_sites,
        summary_df.to_dict("records"),
        missing_df.to_dict("records")
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

    # Handle empty / None input quickly: everything missing in both roles
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

    # Keep only required columns
    directed = grouped_df[["src_netsite","dest_netsite","color"]].dropna(subset=["src_netsite","dest_netsite"]).copy()

    # Collapse multiple observations per **directed** pair to the worst color
    # key = (src, dest) -> worst_color
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

        # As SOURCE
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

        # As DESTINATION
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

    # Sort: primarily by missing (src, then dest), then by "all-red" flags, then by yellows/reds (combined)
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
    # print(f"type: {summary_df.dtypes}")
    return summary_df, missing_expanded_df

