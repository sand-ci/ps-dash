#TODO: aggregation of records(I'm not aggregating, the query isn't optimal but represents what I want to do)
#TODO: make a switch between latency, traceroute, packetloss?

import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
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
TRC_PARQUET = Path("parquet/raw/traceroutes_valid.parquet")
TRC_TTL = timedelta(hours=2)
TO_DATE = datetime.utcnow()
FROM_DATE = TO_DATE - timedelta(hours=2)


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

def readParquetAudit(pq):
        """
        The function reads parquet file that is updated every 24 \
        hours with the data from psConfig about expected hosts and tests \
        results in the Elasticsearch.
        """
        parquet_path = 'parquet/audited_hosts.parquet'
        try: 
            print("Reading the parquet file with hosts audit...")
            df = pq.readFile(parquet_path)
            print(f"Host audit data from parquet file: {df}")
            return df
        except Exception as err:
            print(err)
            print(f"Problems with reading the file {parquet_path}")

def readParquetHostsCRIC(pq):
    """
    The function reads parquet file that is updated every 24 \
    hours with the data about perfSONARs from CRIC.
    """
    parquet_path = 'parquet/raw/CRICDataHosts.parquet'
    try: 
        print("Reading the parquet file with CRIC data...")
        lst = pq.readFile(parquet_path)['host'].tolist()
        return lst
    except Exception as err:
        print(err)
        print(f"Problems with reading the file {parquet_path}")
        
def readParquetSubnetsCRIC(pq):
    parquet_path = 'parquet/raw/CRICDataOPNSubnets.parquet'
    try: 
        print("Reading the parquet file with CRIC OPN subnets data...")
        df = pq.readFile(parquet_path)
        return df
    except Exception as err:
        print(err)
        print(f"Problems with reading the file {parquet_path}")


def ip_in_any(ip, networks):
    return any(ip_address(ip) in ip_network(net) for net in networks)


def query_valid_trace_data(hours, parquet):
    """
    As some tests might run not on perfsonar hosts we are getting wrong image about trace data. 
    This function extracts data only for OPN/T1 sites from Elasticsearch,
    and filters the data to return tests for OPN ip addresses mentioned in CRIC.
    Also writes:
      - invalid_ips_perfsonar.json  -> {site: [IPs not in CRIC OPN ranges]}
      - valid_ips_perfsonar_tested.json -> {site: [IPs that match CRIC OPN ranges]}
    """
    print("Quering data...")
    traceroute_records = qrs.queryOPNTraceroutes(FROM_DATE.strftime(hp.DATE_FORMAT), TO_DATE.strftime(hp.DATE_FORMAT), T1_NETSITES+["pic-LHCOPNE"]) 
    cric_T1_networks = readParquetSubnetsCRIC(parquet)
    cric_T1_networks = cric_T1_networks.set_index("site")["subnets"].to_dict()
    valid_traceroutes = []

    invalid_ips_by_site = defaultdict(set)
    valid_ips_by_site = defaultdict(set)
    for record in traceroute_records:
        srcs = record.get('src_ipvs', [])
        dsts = record.get('dst_ipvs', [])

        src_site = record['src_netsite'].upper()
        dst_site = record['dest_netsite'].upper()
        record['src_netsite'] = src_site
        record['dest_netsite'] = dst_site
        
        src_cric_info = cric_T1_networks.get(src_site, [])
        dest_cric_info = cric_T1_networks.get(dst_site, [])

        src_nonempty = [ip for ip in srcs if ip]
        dst_nonempty = [ip for ip in dsts if ip]
        opn_ips_srcs = [ip_in_any(ip, src_cric_info) for ip in src_nonempty]
        opn_ips_dsts = [ip_in_any(ip, dest_cric_info) for ip in dst_nonempty]

        # check whether ips from elasticsearch are in OPN subnets mentioned in CRIC
        for ip, ok in zip(src_nonempty, opn_ips_srcs):
            (valid_ips_by_site if ok else invalid_ips_by_site)[src_site].add(ip)
        for ip, ok in zip(dst_nonempty, opn_ips_dsts):
            (valid_ips_by_site if ok else invalid_ips_by_site)[dst_site].add(ip)

        if any(opn_ips_srcs) and any(opn_ips_dsts):
            valid_traceroutes.append(record)

    print(f"Before filtering Elasticsearch traceroute records (according to CRIC): {len(traceroute_records)}")
    print(f"After filtering: {len(valid_traceroutes)}")

    return valid_traceroutes

def _is_fresh(p: Path, ttl: timedelta) -> bool:
    if not p.exists():
        return False
    mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    return (datetime.now(timezone.utc) - mtime) < ttl

def load_or_query_traceroutes(hours: int, parquet: Parquet) -> pd.DataFrame:
    """
    Returns a DataFrame of valid traceroute records.
    Re-queries ES only if the local parquet is older than TRC_TTL.
    """
    if _is_fresh(TRC_PARQUET, TRC_TTL):
        print("Using cached traceroute data...")
        return pd.read_parquet(TRC_PARQUET)

    # Not fresh – query and persist
    print("Querying traceroute data...")
    records = query_valid_trace_data(hours=hours, parquet=parquet)
    df = pd.DataFrame(records)
    TRC_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(TRC_PARQUET, index=False)
    return df

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
    pq = Parquet()
    alarm_cnt = pq.readFile('parquet/alarmsGrouped.parquet')
    _, sites_status = generateStatusTable(alarm_cnt)

    # initial data
    traceroute_records = load_or_query_traceroutes(hours=4, parquet=pq)
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
    mesh_config = readParquetPsConfig(pq)
    all_hosts_in_configs = mesh_config['Host'].unique()
    print(f"All hosts in psConfig: {len(all_hosts_in_configs)}\n")
    
    print(mesh_config.head(10))

    print(" --- getting PerfSonars from WLCG CRIC ---")
    all_cric_perfsonar_hosts = readParquetHostsCRIC(pq)
    print(f"All perfSONAR hosts in CRIC: {len(all_cric_perfsonar_hosts)}\n")
    # UI
    return html.Div(children=[
        html.Div(id="div-configs", children=[
            dcc.Store(id="traceroutes-grouped", data={}),
            
            html.Div(children=[
                # Title
                dcc.Store(id="valid-data", data=traceroutes_dict),
                html.Div([
                    html.H1("T1/OPN Network Connectivity Monitor", 
                            className="mb-1", 
                            style={"color": "#2c3e50", "font-weight": "bold"}),
                    html.H3("Traceroute Test Results — Last 2 Hours", 
                        className="text-secondary", 
                        style={"font-weight": "normal"}),

                    # Data period displayed directly under the subtitle (compact and centered)
                    html.Div([
                    html.I(className="fas fa-clock me-2 text-muted"),
                    html.Small(f"Data period: {FROM_DATE.strftime('%Y-%m-%d %H:%M')} — {TO_DATE.strftime('%Y-%m-%d %H:%M')} UTC", className="text-muted")
                    ], className="text-center mb-3", style={"font-size": "0.9rem"}),

                ], className="text-center mt-1 mb-3"),

                # Controls row
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
                                            # Action buttons first, then compact date/update below (right-aligned)
                                            html.Div([
                                                dbc.Button("Search", id="btn-search", className="me-2 mb-1", n_clicks=0),
                                                dbc.Button("Select all", id="btn-select-all", className="me-2 mb-1", n_clicks=0),
                                                dbc.Button("Clear", id="btn-clear", color="secondary", n_clicks=0),
                                            ], className="d-inline-block"),

                                            # compact date removed from here (moved above subtitle)
                                        ], className="d-flex flex-column align-items-end justify-content-md-end mt-2 mt-md-0")
                                    )
                                ], className="mb-1"),

                                # Graph
                                dbc.Row(
                                    dbc.Col(
                                        dcc.Loading(
                                            id="loading-map",
                                            type="default",
                                            color='#00245A',
                                            children=[
                                                    dcc.Graph(id='traceroute-map', figure=buildMap(sites_status, True, grouped),
                                                            style={'height': '85vh'})
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
                            }
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
                
            ], className="p-1 site boxwithshadow page-cont m-3"),
        ]),
            
            
        
        
        html.Div(id="div-audit", children=[
            html.H2("pSConfig Web Admin Hosts Audit", className="mt-2"),
            html.H4("Host auditing can take between 2 and 5 minutes and is performed every 24 hours. During the audit, the host undergoes several tests, based on which it is assigned a status.", className="text mt-2", style={"font-style": "italic", "color": "#A60F0F"}),
            dbc.Row([
                        
                dcc.Store(id="audit-data"),
                dcc.Store(id="last-audit"),
                dcc.Store(id="config-hosts", data=all_hosts_in_configs),
                dcc.Store(id="cric-hosts", data=all_cric_perfsonar_hosts),
                
                
                                
                dbc.Col([
                    html.Div(id="last-audit", className="text-secondary mb-2")
                    ]),
                

                dbc.Col(
                    html.Div(
                        dbc.ButtonGroup([
                            dbc.Button("Audit", id="audit-btn", className="me-2", n_clicks=0, disabled=True),
                            dbc.Button("Open PWA ↗", href="https://psconfig.opensciencegrid.org/#!/configs/58f74a4df5139f0021ac29d6",
                                    className="me-2", target="_blank", color="secondary"),
                        ])
                    ),
                    md=4, className="d-flex align-items-center justify-content-md-end mt-2 mt-md-0"
                )
            ]),        
            
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
            dcc.Loading(
                id="loading-charts",
                type="default",
                color="#00245A",
                delay_show=300,  # optional: avoid flicker
                children=dbc.Row([
                    dbc.Col(dcc.Graph(id="donut-status"), md=6),
                    dbc.Col(dcc.Graph(id="bar-status"), md=6),
                ], className="mb-2")
            ),

            # ---------- Table ----------
            dcc.Loading(
                id="loading-table",
                type="default",
                color="#00245A",
                delay_show=300,
                parent_style={"position": "relative"},  # keeps overlay scoped to this block
                children=html.Div(id="audit-table")
            ),
        ], className="p-1 site boxwithshadow page-cont m-3")
 ])
    
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
    Output("last-audit", "children"),
    Output("audit-data", "data"),


    # Input("audit-btn", "n_clicks"),
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
        df = readParquetAudit(pq)
        data, last_modified = df.to_dict("records"), datetime.fromtimestamp(Path("parquet/audited_hosts.parquet").stat().st_mtime, tz=timezone.utc)
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
    # map to pretty groups; unknowns -> "Other"
    gdf["status_group"] = (
        gdf["status"].map(STATUS_PRETTY).fillna("Other")
    )

    donut_counts = gdf.groupby("status_group").size().reset_index(name="count")
    donut_counts["color_key"] = donut_counts["status_group"].map(lambda s: STATUS_COLORS.get(s, STATUS_COLORS["Other"]))
    fig_donut = px.pie(
        donut_counts, names="status_group", values="count",
        hole=0.55, title="Hosts by Status"
    )
    # custom colors
    fig_donut.update_traces(marker=dict(colors=[STATUS_COLORS.get(s, STATUS_COLORS["Other"])
                                                for s in donut_counts["status_group"]]),
                            textposition="inside")
    fig_donut.update_layout(showlegend=True)

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


    button = ctx.split('.')[0]


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

