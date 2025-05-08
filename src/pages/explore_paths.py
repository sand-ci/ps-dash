import dash
from dash import Dash, html, dcc, Input, Output, Patch, callback, State, ctx, dash_table, dcc, html
import dash_bootstrap_components as dbc
import pandas as pd

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

import utils.helpers as hp
import model.queries as qrs
from model.Alarms import Alarms
from utils.parquet import Parquet
from utils.helpers import timer


def title():
    return f"Search & explore"



def description(q=None):
    return f"Explore the alarms related to traceroute paths"


pq = Parquet()
alarmsInst = Alarms()
selected_keys = ['ASN path anomalies']


dash.register_page(
    __name__,
    path_template="/explore-paths",
    title=title,
    description=description,
)

# Utility: robust parquet reader with error handling
def read_parquet_safe(path):
    try:
        df = pd.read_parquet(path)
        if df.empty:
            print(f"Warning: {path} is empty.")
        return df
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return pd.DataFrame()

# Utility: common filter for DataFrames
def apply_common_filters(df, selected_asns=None, selected_sites=None, anomalies_col='anomalies', src_col='src_netsite', dest_col='dest_netsite'):
    if selected_asns:
        df = df[df[anomalies_col].apply(lambda asn_list: any(str(a) in [str(x) for x in asn_list] for a in selected_asns))]
    if selected_sites:
        df = df[(df[src_col].isin(selected_sites)) | (df[dest_col].isin(selected_sites))]
    return df

def get_dropdown_data(asn_anomalies, pivotFrames):
    # sites
    unique_sites = pd.unique(asn_anomalies[['src_netsite', 'dest_netsite']].values.ravel()).tolist()

    sitesDropdownData = [{"label": str(a), "value": a} for a in sorted(list(set(unique_sites)))]

    # ASNs
    unique_asns = pd.unique(asn_anomalies['anomalies'].explode()).tolist()
    unique_asns = [str(a) for a in unique_asns if str(a) != 'nan']
    asnsDropdownData = [{"label": str(a), "value": str(a)} for a in sorted(unique_asns, key=lambda x: str(x))]

    print(f"Unique sites: {len(sitesDropdownData)}, Unique ASNs: {len(asnsDropdownData)}")

    return sitesDropdownData, asnsDropdownData


@timer
def layout(**other_unknown_query_strings):
    asn_anomalies = read_parquet_safe('parquet/frames/ASN_path_anomalies.parquet')

    if asn_anomalies.empty:
        return dbc.Row([
            dbc.Col([
                html.Div([
                    html.H1("No Data Available", className="text-center"),
                    html.P("There is currently no data to display on this page. Please check back later.", className="text-center")
                ], className="boxwithshadow page-cont ml-1 p-1")
            ], xl=12, lg=12, md=12, sm=12, className="mb-1 flex-grow-1")
        ])

    dateFrom, dateTo = hp.defaultTimeRange(days=2)
    frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
    period_to_display = hp.defaultTimeRange(days=2, datesOnly=True)

    # Skip plot creation if there is no data
    if asn_anomalies.empty:
        dataTables = html.Div([
            html.P("No data available for the selected criteria.", className="text-center")
        ])
        return dbc.Row([
            dbc.Col([
                html.Div([
                    html.H1("No Data Available", className="text-center"),
                    html.P("There is currently no data to display on this page. Please check back later.", className="text-center")
                ], className="boxwithshadow page-cont ml-1 p-1")
            ], xl=12, lg=12, md=12, sm=12, className="mb-1 flex-grow-1")
        ])

    dataTables = generate_data_tables(selected_keys, asn_anomalies)

    heatmap_fig = get_heatmap_fig(asn_anomalies, dateFrom, dateTo)
    parallel_cat_fig = get_parallel_cat_fig([], [])

    sitesDropdownData, asnsDropdownData = get_dropdown_data(asn_anomalies, pivotFrames)

    return dbc.Row([
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div([
                        html.H1(f"ASN Transition Effects"),
                        html.P('The plot shows how ASNs were replaced in the period of 2 days. The data is based on the alarms of type "ASN path anomalies"', style={"font-size": "1.2rem"})
                    ], className="l-h-3 p-2"),
                    dcc.Loading(
                        dcc.Graph(figure=parallel_cat_fig, id="asn-sankey"), color='#00245A'),
                ], className="boxwithshadow page-cont ml-1 p-1")
            ], xl=6, lg=12, md=12, sm=12, className=" mb-1 flex-grow-1",
            ),
            dbc.Col([
                html.Div(id="asn-alarms-container", children=[
                    html.Div([
                        html.H1(f"ASN-path Anomalies Affected Links"),
                        html.P('The plot shows the number of new ASNs that appeared between two sites. The data is based on the alarms of type "ASN path anomalies"', style={"font-size": "1.2rem"})
                    ], className="l-h-3 p-2"),
                    dcc.Loading(
                        dcc.Graph(figure=heatmap_fig, id="asn-heatmap", style={"max-width": "1000px", "margin": "0 auto"}),
                        color='#00245A')
                ], className="boxwithshadow page-cont ml-1 p-1")
            ], xl=6, lg=12, md=12, sm=12, className="mb-1 flex-grow-1")
        ], className="gx-4 d-flex", justify="between", align="start"),
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        html.H1(f"Search for specific site and/or ASN and get all related alarms based on the traceroute measurements",
                                className="l-h-3 pl-2"),
                        html.P(
                            f'Alarms generated in the period: {period_to_display[0]} - {period_to_display[1]} ',
                            style={"padding-left": "1.5%", "font-size": "14px"})
                    ], align="center", className="text-left pair-details rounded-border-1"),
                ], justify="start", align="center"),
                html.Br(),
                html.Br(),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='paths-sites-dropdown', options=sitesDropdownData,
                                     placeholder="Search for a site"),
                    ]),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dcc.Dropdown(multi=True, id='paths-asn-dropdown', options=asnsDropdownData,
                                     placeholder="Search ASNs"),
                    ]),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        dbc.Button("Search", id="search-button", color="secondary", 
                                   className="mlr-2", style={"width": "100%", "font-size": "1.5em"},
                                   title="Click to apply the selected filters to the plots and tables.")
                    ])
                ]),
            ], lg=12, md=12),
        ], className="p-1 site boxwithshadow page-cont mb-1", justify="center", align="center"),
        html.Br(),
        html.Br(),
        dbc.Row([
            html.Div([
                dbc.Row([
                    html.H1(f"List of alarms", className="text-center"),
                    html.Hr(className="my-2"),
                    html.Br(),
                    dcc.Loading(
                        html.Div(id='paths-results-table', children=dataTables),
                        style={'height': '0.5rem'}, color='#00245A')
                ], className="m-2"),
            ], className="site page-cont"),
        ], className="p-1 site boxwithshadow page-cont mb-1", justify="center", align="center"),
        html.Br(),
        html.Br(),
    ], className=' main-cont', align="center", style={"padding": "0.5% 1.5%"})


@timer
def load_initial_data(selected_keys, asn_anomalies):
    dateFrom, dateTo = hp.defaultTimeRange(days=2)
    frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
    dataTables = []

    for event in sorted(selected_keys):
        if event in pivotFrames.keys():
            df = pivotFrames[event]
            if len(df) > 0:
                dataTables.append(generate_tables(frames[event], df, event, alarmsInst))
    fig = get_parallel_cat_fig([], [])
    return [dataTables, fig]


@dash.callback(
    Output('asn-sankey', 'figure'),
    Output('asn-heatmap', 'figure'),
    Output('paths-results-table', 'children'),
    [
        Input("search-button", "n_clicks"),
    ],
    [
        State("paths-asn-dropdown", "value"),
        State("paths-sites-dropdown", "value"),
    ],
    prevent_initial_call=True
)
def update_figures(n_clicks, asnStateValue, sitesStateValue):
    if n_clicks is not None:
        asn_anomalies = read_parquet_safe('parquet/frames/ASN_path_anomalies.parquet')
        dateFrom, dateTo = hp.defaultTimeRange(days=2)

        sitesState = sitesStateValue if sitesStateValue else []
        asnState = asnStateValue if asnStateValue else []

        parallel_cat_fig = get_parallel_cat_fig(sitesState, asnState)
        heatmap_fig = get_heatmap_fig(asn_anomalies, dateFrom, dateTo, selected_asns=asnState, selected_sites=sitesState)
        datatables = create_data_tables(sitesState, asnState, selected_keys)
        return parallel_cat_fig, heatmap_fig, datatables

    return dash.no_update


def filterASN(df, selected_asns=[], selected_sites=[]):
    # Use the new utility for filtering
    return apply_common_filters(df, selected_asns, selected_sites)


@timer
def create_data_tables(sitesState, asnState, selected_keys):
    dateFrom, dateTo = hp.defaultTimeRange(days=2)
    frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
    dataTables = []
    for event in sorted(selected_keys):
        df = pivotFrames[event]

        df = df[df['tag'].isin(sitesState)] if len(sitesState) > 0 else df
        if 'diff' in df.columns and len(asnState) > 0:
            df = df[df['diff'].isin(asnState)]
        elif 'asn' in df.columns and len(asnState) > 0:
            df = df[df['asn'].isin(asnState)]
        elif 'anomalies' in df.columns and len(asnState) > 0:
            df = df[df['anomalies'].isin(asnState)]

        if 'src_site' in df.columns and 'dest_site' in df.columns and len(sitesState) > 0:
            df = df[(df['src_site'].isin(sitesState)) | (df['dest_site'].isin(sitesState))]

        if len(df) > 0:
            dataTables.append(generate_tables(frames[event], df, event, alarmsInst))

    if len(dataTables)==0:
        dataTables.append(html.P(f'There are no alarms related to the selected criteria', 
                                style={"padding-left": "1.5%", "font-size": "14px"}))

    return html.Div(dataTables)

@timer
def generate_data_tables(selected_keys, asn_anomalies):
    dateFrom, dateTo = hp.defaultTimeRange(days=2)
    frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
    dataTables = []

    for event in sorted(selected_keys):
        if event in pivotFrames.keys():
            df = pivotFrames[event]
            if len(df) > 0:
                dataTables.append(generate_tables(frames[event], df, event, alarmsInst))

    if len(dataTables) == 0:
        dataTables.append(html.P(
            f'There are no alarms related to the selected criteria',
            style={"padding-left": "1.5%", "font-size": "14px"}
        ))

    return html.Div(dataTables)


def get_heatmap_fig(asn_anomalies, dateFrom, dateTo, selected_asns=[], selected_sites=[]):
    return create_anomalies_heatmap(asn_anomalies, dateFrom, dateTo, selected_asns=selected_asns, selected_sites=selected_sites)


def get_parallel_cat_fig(sitesState, asnState):
    return build_parallel_categories_plot(sitesState, asnState)


@timer
def create_anomalies_heatmap(asn_anomalies, dateFrom, dateTo, selected_asns=[], selected_sites=[]):
    df = asn_anomalies.copy()
    df = df[df['to_date'] >= dateFrom]
    df = apply_common_filters(df, selected_asns, selected_sites)

    if len(df) > 0:
        # Create a summary table with counts and ASN list per IPv6 and IPv4
        heatmap_summary = df.groupby(['src_netsite', 'dest_netsite', 'ipv6']).agg(
                        asn_details=('anomalies', lambda x: [item for sublist in x for item in set(sublist)])
                    ).reset_index()

        # heatmap_summary['asn_details'] = heatmap_summary['asn_details'].apply(lambda x: ', '.join(map(str, x)))
        heatmap_summary['asn_details'] = heatmap_summary['asn_details'].apply(lambda x: list(set(x)))
        heatmap_summary['count'] = heatmap_summary['asn_details'].apply(len)

        distinct_pairs = heatmap_summary[['src_netsite', 'dest_netsite']].drop_duplicates()

        # Function to create a formatted string with ASN details and count the total unique ASNs
        def format_asn_string_and_total_count(group):
            ipv4_asns = group[group['ipv6'] == False]['asn_details'].explode().unique().tolist()
            ipv6_asns = group[group['ipv6'] == True]['asn_details'].explode().unique().tolist()
            
            # Create formatted ASN strings
            formatted_str = ""
            if ipv4_asns:
                formatted_str += f"IPv4 -> <b>{', '.join(map(str, ipv4_asns))}</b>; \n "
            if ipv6_asns:
                formatted_str += f" IPv6 -> <b>{', '.join(map(str, ipv6_asns))}</b>".rstrip(',')
            
            # Calculate total unique ASNs across both versions
            total_unique_asns = len(set(ipv4_asns).union(set(ipv6_asns)))

            return formatted_str.strip(), total_unique_asns

        # Apply function to calculate ASNs and total count
        distinct_pairs[['asn_details_str', 'total_count']] = distinct_pairs.apply(
            lambda row: format_asn_string_and_total_count(
                heatmap_summary[(heatmap_summary['src_netsite'] == row['src_netsite']) &
                                (heatmap_summary['dest_netsite'] == row['dest_netsite'])]
            ), axis=1, result_type='expand'
        )

        heatmap_pivot = distinct_pairs.pivot(index='src_netsite', columns='dest_netsite', values='total_count').fillna(0)

        # Create a custom data matrix for ASNs grouped by IPv6 for hover display
        custom_data = distinct_pairs.pivot(index='src_netsite', columns='dest_netsite', values='asn_details_str').fillna('')
        # Create the heatmap using Plotly
        fig = px.imshow(
            heatmap_pivot,
            labels=dict(x="Destination", y="Source", color="Count"),
            color_continuous_scale="BuPu",
            text_auto=True
        )

        fig.update_traces(customdata=custom_data.values)
        fig.update_traces(
            hovertemplate="<br>".join([
                "Source: %{y}",
                "Destination: %{x}",
                "ASNs: %{customdata}"
            ])
        )

        # Update layout for better appearance
        fig.update_layout(
            # title="ASN path anomalies summary",
            xaxis_title="Destination",
            yaxis_title="Source",
            xaxis=dict(title=dict(text="Destination", font=dict(size=16))),
            height=600,
            plot_bgcolor='rgba(0,0,0,0)',
        )

    else:
        fig = px.imshow(pd.DataFrame())
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            annotations=[
                dict(
                    text="No data available for the selected criteria",
                    showarrow=False,
                    font=dict(size=16),
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                )
            ]
        )


    return fig


@timer
# '''Takes the sites from the dropdown list and generates a Dash datatable'''
def generate_tables(frame, pivotFrame, event, alarmsInst):
    ids = pivotFrame['id'].values
    dfr = frame[frame.index.isin(ids)]
    dfr = alarmsInst.formatDfValues(dfr, event).sort_values('to', ascending=False)
    # print('Paths page,', event, "Number of alarms:", len(dfr))

    element = html.Div([
        html.Br(),
        html.H3(event.upper()),
        dash_table.DataTable(
            data=dfr.to_dict('records'),
            columns=[{"name": i, "id": i, "presentation": "markdown"} for i in dfr.columns],
            markdown_options={"html": True},
            id=f'paths-search-tbl-{event}',
            page_current=0,
            page_size=20,
            style_cell={
                'padding': '2px',
                'whiteSpace': 'pre-line',
                "font-size": "1.4rem",
            },
            style_header={
                'backgroundColor': 'white',
                'fontWeight': 'bold'
            },
            style_data={
                'height': 'auto',
                'lineHeight': '1.5rem',
                'overflowX': 'auto'
            },
            style_table={
                    'overflowY': 'auto',
                    'overflowX': 'auto'
            },
            filter_action="native",
            filter_options={"case": "insensitive"},
            sort_action="native",
        ),
    ], className='single-table')

    return element


@timer
# '''Creates a list of labels with network providers'''
def addNetworkOwners(df, asn_list):
    owners = qrs.getASNInfo(asn_list)
    customdata = []
    for asn in asn_list:
        if asn in owners.keys():
            customdata.append(f'{asn}: {owners[asn]}')
        else:
            customdata.append(f'{asn}: Unknown')
    return customdata


@timer
def build_parallel_categories_plot(sitesState, asnState) -> go.Figure:
    """
    Query all docs with transitions existing and to_date==to_date,
    then build one combined parallel-categories figure.
    Handles list-type 'previously_used_asn' and 'new_asn' columns.
    """
    # Read pre-stored data from parquet file generated by Updater()
    try:
        df = read_parquet_safe('parquet/asn_path_changes.parquet')
    except Exception as e:
        print("Error reading parquet file:", e)
        return go.Figure()
    if df.empty:
        return go.Figure()

    # Explode both list columns to get all combinations
    df = df.explode('previously_used_asn')
    df = df.explode('new_asn')

    # Convert to string for display
    df['previously_used_asn'] = df['previously_used_asn'].astype(str)
    df['new_asn'] = df['new_asn'].astype(str)

    # Filtering
    if len(sitesState) > 0 and len(asnState) > 0:
        df = df[((df['source_site'].isin(sitesState)) | (df['destination_site'].isin(sitesState))) &
                ((df['new_asn'].isin(asnState)) | (df['previously_used_asn'].isin(asnState)))]
    elif len(sitesState) > 0:
        df = df[(df['source_site'].isin(sitesState)) | (df['destination_site'].isin(sitesState))]
    elif len(asnState) > 0:
        df = df[((df['new_asn'].isin(asnState)) | (df['previously_used_asn'].isin(asnState)))]

    if df.empty:
        return go.Figure()

    df['asn_code'] = pd.Categorical(df['new_asn']).codes
    unique_asn_count = df['asn_code'].nunique()

    # Dynamically determine font size based on label length
    label_fields = ["source_site", "previously_used_asn", "new_asn", "destination_site"]
    max_label_len = 0
    for field in label_fields:
        if field in df.columns:
            max_label_len = max(max_label_len, df[field].astype(str).map(len).max())
    # Set font size: shrink if label is long
    if max_label_len > 30:
        font_size = 10
    elif max_label_len > 20:
        font_size = 12
    else:
        font_size = 14

    if unique_asn_count == 0:
        return go.Figure()
    elif unique_asn_count == 1:
        fig = px.parallel_categories(
            df,
            dimensions=label_fields,
            labels={
                "source_site": "Source site",
                "previously_used_asn": "Previously used ASN",
                "new_asn": "New ASN",
                "destination_site": "Destination site"
            }
        )
    else:
        palette = px.colors.sequential.Viridis_r
        fig = px.parallel_categories(
            df,
            color='asn_code',
            color_continuous_scale=palette[:unique_asn_count],
            dimensions=label_fields,
            labels={
                "source_site": "Source site",
                "previously_used_asn": "Previously used ASN",
                "new_asn": "New ASN",
                "destination_site": "Destination site"
            }
        )

    fig.update_layout(
        margin=dict(t=20, b=20, l=150, r=150),
        height=600,
        showlegend=False,
        coloraxis_showscale=False,
        font=dict(size=font_size)
    )

    return fig



def generate_figures(asn_anomalies):
    dateFrom, dateTo = hp.defaultTimeRange(days=2)

    # Generate the heatmap figure
    heatmap_fig = get_heatmap_fig(asn_anomalies, dateFrom, dateTo)

    # Generate the parallel categories figure
    parallel_cat_fig = get_parallel_cat_fig([], [])

    return heatmap_fig, parallel_cat_fig