import dash
from dash import html
from dash import Dash, html, dcc, Input, Output, Patch, callback, State, ctx, dash_table, dcc, html

import urllib3

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import dash_bootstrap_components as dbc

import model.queries as qrs
import utils.helpers as hp

urllib3.disable_warnings()



def title(q=None):
    return f"ASN-path anomalies"



def description(q=None):
    return f"Visual represention of an ASN-path anomaly"



dash.register_page(
    __name__,
    path_template="/anomalous_paths/<q>",
    title=title,
    description=description,
)


def layout(q=None, **other_unknown_query_strings):
    return html.Div([
        dcc.Location(id='url', refresh=False),
        dcc.Store(id='alarm-data-store'),
        html.Div(id='asn-anomalies-content'),
        html.Div([
          html.Div([
            html.H1(f"The most recent ASN paths"),
            html.P('The plot shows new ASNs framed in white. The data is based on the alarms of type "ASN path anomalies"', style={"font-size": "1.2rem"})
          ], className="l-h-3 p-2"),
          dcc.Loading(id='loading-spinner', type='default', children=[
              html.Div(id='asn-anomalies-graphs')
          ], color='#00245A'),
        ], className="l-h-3 p-2 boxwithshadow page-cont ml-1 p-1")
    ], style={"padding": "0.5% 1.5%"})


@callback(
    Output('alarm-data-store', 'data'),
    Input('url', 'pathname')
)
def update_store(pathname):
    if pathname:
        # Extract the parameters from the URL path
        path_parts = pathname.split('/')
        if len(path_parts) > 2 and '=' in path_parts[2]:
            params = path_parts[2].split('&')
            query_params = {param.split('=')[0]: param.split('=')[1] for param in params}
            return query_params
    return {}


@callback(
    Output('asn-anomalies-graphs', 'children'),
    Input('alarm-data-store', 'data')
)
def update_graphs(query_params):
    if query_params:
        src = query_params.get('src_netsite')
        dest = query_params.get('dest_netsite')
        dt = query_params.get('dt')
        print(src, dest, dt)
        if src and dest:
            data = qrs.query_ASN_anomalies(src, dest)

            if len(data) > 0:
                if len(data['ipv6'].unique()) == 2:
                    ipv6_figure = generate_plotly_heatmap_with_anomalies(data[data['ipv6'] == True])
                    ipv4_figure = generate_plotly_heatmap_with_anomalies(data[data['ipv6'] == False])
                    
                    figures = html.Div([
                        dbc.Row([
                            dcc.Graph(figure=ipv4_figure, id="asn-sankey-ipv4"),
                            dcc.Graph(figure=get_heatmap_fig(src, dest, dt, 0), id="asn-path-prob-ipv4"),
                        ], className="graph-pair"),
                        dbc.Row([
                            dcc.Graph(figure=ipv6_figure, id="asn-sankey-ipv6"),
                            dcc.Graph(figure=get_heatmap_fig(src, dest, dt, 1), id="asn-path-prob-ipv6"),
                        ], className="graph-pair"),
                    ], className="responsive-graphs")
                else:
                    ipv4_figure = generate_plotly_heatmap_with_anomalies(data)
        
                    figures =  html.Div([
                        dbc.Row([
                            dbc.Col(dcc.Graph(figure=ipv4_figure, id="asn-sankey-ipv4")),
                            dbc.Col(dcc.Graph(figure=get_heatmap_fig(src, dest, dt, -1), id="asn-path-prob-ipv4")),
                        ], className="graph-pair")
                    ], className="responsive-graphs")

                return html.Div(figures)
            else:
                return html.Div([
                    html.H1(f"No data found for alarm {src} to {dest}"),
                    html.P('No data was found for the alarm selected. Please try another alarm.', style={"font-size": "1.2rem"})
                ], className="l-h-3 p-2 boxwithshadow page-cont ml-1 p-1")
    return html.Div()




def get_heatmap_fig(src, dest, dt, ipv) -> go.Figure:
    """
    Fetch the document with this alarm_id, and render its heatmap.
    """
    ipv_str = None
    if ipv >= 0:
        ipv_str = {
            "term": {
                "ipv6": ipv
            }
        }

    try:
        q = {
                "bool": {
                      "must": [
                        {
                          "exists": {
                            "field": "transitions"
                          }
                        },
                        {
                          "range": {
                            "to_date": {
                              "gte": dt,
                              "format": "strict_date_optional_time"
                            }
                          }
                        },
                        {
                          "term": {
                            "src_netsite.keyword": src
                          }
                        },
                        {
                          "term": {
                            "dest_netsite.keyword": dest
                          }
                        },
                        *([ipv_str] if ipv_str else [])
                      ]
                    }
          }
        # print(str(q).replace('\'', '"'))
        res = hp.es.search(index='ps_traces_changes', query=q)
    except Exception:
        # not found / error
        return go.Figure()

    doc = res['hits']['hits'][0]["_source"]
    hm  = doc["heatmap"]

    fig = go.Figure(go.Heatmap(
        z=hm["probs"],
        x=[f"pos {p}" for p in hm["positions"]],
        y=[str(a)   for a in hm["asns"]],
        colorscale=[[0.0, "white"], [0.001, "#caf0f8"], [0.5, "#00b4d8"], [1.0, "#03045e"]],\
        zmin=0, zmax=1,
        xgap=1, ygap=1,
        hovertemplate="ASN %{y}<br>Position %{x}<br>Prob %{z:.2%}<extra></extra>"
    ))
    fig.update_layout(
        title=(
            f"{doc['src_netsite']} → {doc['dest_netsite']}  "
            f"(IPv6={doc['ipv6']}), anomalies={doc['anomalies']}"
        ),
        xaxis_title="Position in Path",
        yaxis_title="ASN",
        height=500, margin=dict(t=80, l=80, r=80, b=50)
    )

    return fig


def generate_plotly_heatmap_with_anomalies(subset_sample):
    columns = ['src_netsite', 'dest_netsite', 'anomalies', 'ipv6']
    src_site, dest_site, anomaly, ipv = subset_sample[columns].values[0]
    ipv = 'IPv6' if ipv else 'IPv4'

    subset_sample['last_appearance_path'] = pd.to_datetime(subset_sample['last_appearance_path'], errors='coerce')

    # Create a short format date column for plotting
    subset_sample['last_appearance_short'] = subset_sample['last_appearance_path'].dt.strftime('%H:%M %d-%b')

    print('Size of dataset:', len(subset_sample))
    max_length = subset_sample["path_len"].max()

    # Convert the path list into a pivot DataFrame
    pivot_df = pd.DataFrame(
        subset_sample['repaired_asn_path'].tolist(),
        index=subset_sample.index,
        columns=[f"pos_{i+1}" for i in range(max_length)]
    ).applymap(lambda x: int(x) if isinstance(x, (int, float)) and not pd.isna(x) else x)

    # Map ASNs to colors
    unique_rids = pd.Series(pivot_df.stack().unique()).dropna().tolist()
    if 0 not in unique_rids:
        unique_rids.append(0)
    rid_to_index = {rid: i + 1 for i, rid in enumerate(unique_rids)}
    rid_to_index[np.nan] = 0

    base_colors = px.colors.qualitative.Prism + px.colors.qualitative.Bold
    expanded_colors = (base_colors * (len(unique_rids) // len(base_colors) + 1))[:len(unique_rids)]
    color_list = ['#FFFFFF'] + expanded_colors
    color_list[rid_to_index[0]] = '#000000'

    index_df = pivot_df.applymap(lambda x: rid_to_index.get(x, 0))

    # Prepare hoverlabel background colors
    index_to_color = {i: color_list[i] for i in range(len(color_list))}
    hover_bgcolor = np.array([index_to_color.get(z, '#FFFFFF') for row in index_df.values for z in row]).reshape(index_df.shape)

    # Prepare the heatmap using WebGL
    fig = go.Figure()
    heatmap = go.Heatmap(
        z=index_df.values,
        x=index_df.columns,
        y=subset_sample['last_appearance_short'],  # Use formatted short date for y-axis
        colorscale=color_list,
        zmin=0,
        zmax=len(unique_rids),
        customdata=pivot_df.values,
        hoverlabel=dict(bgcolor=hover_bgcolor),
        hovertemplate="<b>Position: %{x}</b><br><b>ASN: %{customdata}</b><extra></extra>",
        showscale=False,
    )
    fig.add_trace(heatmap)

    # Add annotations for anomalies
    for idx, row in enumerate(pivot_df.values):
        for col_idx, asn in enumerate(row):
            if ~np.isnan(asn):
                if asn in anomaly:
                    fig.add_annotation(
                        x=index_df.columns[col_idx],
                        y=subset_sample['last_appearance_short'].iloc[idx],
                        text=int(asn),
                        showarrow=False,
                        bordercolor='white',
                        font=dict(color='white', size=12, family="Arial", weight='bold'),
                    )
                else:
                    fig.add_annotation(
                        x=index_df.columns[col_idx],
                        y=subset_sample['last_appearance_short'].iloc[idx],
                        text=int(asn),
                        showarrow=False,
                        font=dict(color='white', size=10, family="Arial", weight='bold'),
                    )

    fig.update_layout(
        title=f"ASN path signature between {src_site} and {dest_site} for {ipv} paths",
        xaxis_title='Position',
        yaxis_title='Path Observation Date',
        margin=dict(r=150),
        height=600,
        yaxis=dict(autorange='reversed', type='category', fixedrange=False)
    )

    return fig