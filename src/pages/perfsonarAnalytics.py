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

import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import pandas as pd
from datetime import datetime, timedelta
from pages.cric_extract_OPN import query_valid_trace_data

import utils.helpers as hp
from utils.parquet import Parquet
from utils.utils import buildMap, generateStatusTable, get_color
from dash import dash_table as dt

import psconfig.api
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

dash.register_page(
    __name__,
    path_template="/network-testing",
    title='Network testing',
    description="",
)
T1_NETSITES = [
    "BNL-ATLAS-LHCOPNE", "USCMS-FNAL-WC1-LHCOPNE", "RAL-LCG2-LHCOPN",
    "RRC-KI-T1-LHCOPNE", "JINR-T1-LHCOPNE", "NCBJ-LHCOPN",
    "NLT1-SARA-LHCOPNE", "INFN-T1-LHCOPNE", "NDGF-T1-LHCOPNE",
    "KR-KISTI-GSDC-1-LHCOPNE", "IN2P3-CC-LHCOPNE", "PIC-LHCOPNE",
    "FZK-LCG2-LHCOPNE", "CERN-PROD-LHCOPNE", "TRIUMF-LCG2-LHCOPNE", 'TW-ASGC'
]
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
    
    # print(" --- getting PerfSonars from WLCG CRIC ---")
    # all_cric_perfsonar_hosts = []
    # r = requests.get(
    #     'https://wlcg-cric.cern.ch/api/core/service/query/?json&state=ACTIVE&type=PerfSonar',
    #     verify=False
    # )
    # res = r.json()
    # for _key, val in res.items():
    #     if not val['endpoint']:
    #         print('no hostname? should not happen:', val)
    #         continue
    #     p = val['endpoint']
    #     all_cric_perfsonar_hosts.append(p)
    # print(f"All perfSONAR hosts in CRIC: {len(all_cric_perfsonar_hosts)}\n")

    # print(" --- audit of all hosts from psConfigAdmin ---")
    # result = await hosts_audit(all_hosts_in_configs, all_cric_perfsonar_hosts)
    # grouped_result = group_by_status(result)

    # UI
    return html.Div(children=[
        html.Div(children=[
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
                html.Li("Priority 2 — All tested connections are RED in a role: likely pS misconfiguration at the site."),
                html.Li("Priority 3 — YELLOW connections: destination never reached but path complete (possible firewall/ICMP filtering)."),
            ], className="mb-0")
        ],
        color="light",
        className="mb-2"
    ),

    # ---------- Per-site combined (src + dest) ----------
    html.Div([
        html.H5("Per-site ranking (worst first)", className="mt-2"),
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
            "if": {
                "filter_query": "{all_red_dest_flag} = 'True'",
                "column_id": "all_red_dest_flag"
            },
            "backgroundColor": "#ff000077",
            "color": "white"
        },
    ],
)
    ], className="mb-3"),

    # ---------- Exactly which connections are missing (role-aware) ----------
    html.Div([
        html.H5("Exactly which connections are missing (role-aware)", className="mt-2"),
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
    ], className="p-1 site boxwithshadow page-cont m-3")

    ])

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
        print('df')
        print(df)
        df = df[df["src_netsite"].isin(selected_sites) | df["dest_netsite"].isin(selected_sites)]
        print('df')
        print(df)

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
    print(summary_df)
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
    print(grouped_df)
    print(sites)

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
    print(f"type: {summary_df.dtypes}")
    return summary_df, missing_expanded_df
