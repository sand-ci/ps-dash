import dash
from dash import Dash, html, dcc, Input, Output, Patch, callback, State, ctx, dash_table, dcc, html
import dash_bootstrap_components as dbc
import pandas as pd

import plotly.graph_objects as go
import plotly.express as px

import utils.helpers as hp
import model.queries as qrs
from model.Alarms import Alarms
from utils.parquet import Parquet


def title():
    return f"Search & explore"



def description(q=None):
    return f"Explore the alarms related to traceroute paths"


pq = Parquet()
alarmsInst = Alarms()
selected_keys = ['path changed between sites', 'path changed', 'ASN path anomalies']


dash.register_page(
    __name__,
    path_template="/explore-paths",
    title=title,
    description=description,
)

def get_dropdown_data(asn_anomalies, pivotFrames, changeDf):
    # sites
    unique_sites = pd.unique(asn_anomalies[['src_netsite', 'dest_netsite']].values.ravel()).tolist()
    unique_sites.extend(pivotFrames['path changed'].tag.unique().tolist())

    sitesDropdownData = [{"label": str(a), "value": a} for a in sorted(list(set(unique_sites)))]

    # ASNs
    unique_asns = pd.unique(asn_anomalies['asn_list'].explode()).tolist()
    sortedDf = changeDf[changeDf['jumpedFrom'] > 0].sort_values('count')
    asnsDropdownData = list(set(sortedDf['diff'].unique().tolist() +
                                sortedDf['jumpedFrom'].unique().tolist()))
    asnsDropdownData = list(set(asnsDropdownData + unique_asns))
    asnsDropdownData = [{"label": str(a), "value": a} for a in sorted(asnsDropdownData)]

    print(f"Unique sites: {len(sitesDropdownData)}, Unique ASNs: {len(asnsDropdownData)}")

    return sitesDropdownData, asnsDropdownData


def layout(**other_unknown_query_strings):
    changeDf = pq.readFile('parquet/prev_next_asn.parquet')
    asn_anomalies = pq.readFile('parquet/frames/ASN_path_anomalies.parquet')

    dateFrom, dateTo = hp.defaultTimeRange(2)
    frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
    period_to_display = hp.defaultTimeRange(days=2, datesOnly=True)

    sankey_fig, dataTables = load_initial_data(selected_keys, changeDf, asn_anomalies)
    heatmap_fig = create_anomalies_heatmap(asn_anomalies, dateFrom, dateTo)

    sitesDropdownData, asnsDropdownData = get_dropdown_data(asn_anomalies, pivotFrames, changeDf)

    return dbc.Row([
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div([
                        html.H1(f"Short term path deviations between sites"),
                        html.P('The plot shows how ASNs were replaced in the period of 2 days. The data is based on the alarms of type "path changed"', style={"font-size": "1.2rem"})
                    ], className="l-h-3 p-2"),
                    dcc.Loading(
                        dcc.Graph(figure=sankey_fig, id="asn-sankey"), color='#00245A'),
                ], className="boxwithshadow page-cont ml-1 p-1")
            ], xl=6, lg=12, md=12, sm=12, className=" mb-1 flex-grow-1",
            ),
            dbc.Col([
                html.Div(id="asn-alarms-container", children=[
                    html.Div([
                        html.H1(f"ASN path anomalies"),
                        html.P('The plot shows new ASNs that appeared between two sites. The data is based on the alarms of type "ASN path anomalies"', style={"font-size": "1.2rem"})
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
                                   className="mlr-2", style={"width": "100%", "font-size": "1.5em"})
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


def colorMap(eventTypes):
  colors = ['#75cbe6', '#3b6d8f', '#75E6DA', '#189AB4', '#2E8BC0', '#145DA0', '#05445E', '#0C2D48',
            '#5EACE0', '#d6ebff', '#498bcc', '#82cbf9',
            '#2894f8', '#fee838', '#3e6595', '#4adfe1', '#b14ae1'
            '#1f77b4', '#ff7f0e', '#2ca02c', '#00224e', '#123570', '#3b496c', '#575d6d', '#707173', '#8a8678', '#a59c74',
            ]

  paletteDict = {}
  for i, e in enumerate(eventTypes):
      paletteDict[e] = colors[i]

  return paletteDict


def load_initial_data(sselected_keys, changeDf, asn_anomalies):
    dateFrom, dateTo = hp.defaultTimeRange(2)
    frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
    dataTables = []

    # Filter out non-numeric values before conversion
    changeDf = changeDf[pd.to_numeric(changeDf['jumpedFrom'], errors='coerce').notnull()]
    changeDf['jumpedFrom'] = changeDf['jumpedFrom'].fillna(0).astype(int)
    changeDf['diff'] = changeDf['diff'].astype(int)

    for event in sorted(selected_keys):
        if event in pivotFrames.keys():
            df = pivotFrames[event]
            if len(df) > 0:
                dataTables.append(generate_tables(frames[event], df, event, alarmsInst))

    # Explicitly cast 'jumpedFrom' column to object dtype before assigning 'No data'
    changeDf['jumpedFrom'] = changeDf['jumpedFrom'].astype(object)
    changeDf.loc[changeDf['jumpedFrom'] == 0, 'jumpedFrom'] = 'No data'
    fig = buildSankey([], [], changeDf)

    return [fig, dataTables]


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
        # Define the necessary variables within the callback function
        changeDf = pq.readFile('parquet/prev_next_asn.parquet')
        asn_anomalies = pq.readFile('parquet/frames/ASN_path_anomalies.parquet')
        dateFrom, dateTo = hp.defaultTimeRange(2)

        sitesState = sitesStateValue if sitesStateValue else []
        asnState = asnStateValue if asnStateValue else []

        sankey_fig = buildSankey(sitesState, asnState, changeDf)
        heatmap_fig = create_anomalies_heatmap(asn_anomalies, dateFrom, dateTo, selected_asns=asnState, selected_sites=sitesState)
        datatables = create_data_tables(sitesState, asnState, selected_keys)
        return sankey_fig, heatmap_fig, datatables

    return dash.no_update


def filterASN(df, selected_asns=[], selected_sites=[]):

  if selected_asns:
      s = df.apply(lambda x: pd.Series(x['asn_list']), axis=1).stack().reset_index(level=1, drop=True)
      s.name = 'asn'
      df = df.join(s)
      df = df[df['asn'].isin(selected_asns)]
      df = df.drop('asn', axis=1).drop_duplicates(subset=['alarm_id'])
  if selected_sites:
      df = df[(df['src_netsite'].isin(selected_sites)) | (df['dest_netsite'].isin(selected_sites))]
  
  return df


def create_data_tables(sitesState, asnState, selected_keys):
    dateFrom, dateTo = hp.defaultTimeRange(2)
    frames, pivotFrames = alarmsInst.loadData(dateFrom, dateTo)
    dataTables = []
    for event in sorted(selected_keys):
        df = pivotFrames[event]

        df = df[df['tag'].isin(sitesState)] if len(sitesState) > 0 else df
        if 'diff' in df.columns and len(asnState) > 0:
            df = df[df['diff'].isin(asnState)]
        elif 'asn' in df.columns and len(asnState) > 0:
            df = df[df['asn'].isin(asnState)]
        elif 'asn_list' in df.columns and len(asnState) > 0:
            df = df[df['asn_list'].isin(asnState)]

        if 'src_site' in df.columns and 'dest_site' in df.columns and len(sitesState) > 0:
            df = df[(df['src_site'].isin(sitesState)) | (df['dest_site'].isin(sitesState))]

        if len(df) > 0:
            dataTables.append(generate_tables(frames[event], df, event, alarmsInst))

    if len(dataTables)==0:
        dataTables.append(html.P(f'There are no alarms related to the selected criteria', 
                                style={"padding-left": "1.5%", "font-size": "14px"}))

    return html.Div(dataTables)



def create_anomalies_heatmap(asn_anomalies, dateFrom, dateTo, selected_asns=[], selected_sites=[]):
    print('Creating heatmap', dateFrom, dateTo, selected_asns, selected_sites)
    df = asn_anomalies.copy()
    df = df[df['to_date'] >= dateFrom]
    df = filterASN(df, selected_asns=selected_asns, selected_sites=selected_sites)

    if len(df) > 0:
        # Create a summary table with counts and ASN list per IPv6 and IPv4
        heatmap_summary = df.groupby(['src_netsite', 'dest_netsite', 'ipv6']).agg(
                        asn_details=('asn_list', lambda x: [item for sublist in x for item in set(sublist)])
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


# '''Takes the sites from the dropdown list and generates a Dash datatable'''
def generate_tables(frame, pivotFrame, event, alarmsInst):
    ids = pivotFrame['id'].values
    dfr = frame[frame.index.isin(ids)]
    dfr = alarmsInst.formatDfValues(dfr, event).sort_values('to', ascending=False)
    print('Paths page,', event, "Number of alarms:", len(dfr))

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


# '''Creates a list of labels with network providers'''
def addNetworkOwners(df, labels):
  asns = list(set(df['jumpedFrom'].unique().tolist() +
              df['diff'].unique().tolist()))
  owners = qrs.getASNInfo(asns)

  customdata = []
  for l in labels:
    if l in owners.keys():
      customdata.append(f'<b>{l}</b>: {owners[l]}')
    else:
      customdata.append(l)

  return customdata


# ''' Prepares the data for the Sankey diagram'''
def data4Sankey(sandf):
    sandf = sandf[~sandf['diff'].isin(['No data', '0', 0])]
    typical = [f't{n}' for n in sandf['jumpedFrom'].unique().tolist()]
    diff = [f'd{n}' for n in sandf['diff'].unique().tolist()]
    src = [f'src_{n}' for n in sandf['src_site'].unique().tolist()]
    dst = [f'dst_{n}' for n in sandf['dest_site'].unique().tolist()]
    nodes = src + dst + typical + diff
    nodesdict = {n: i for i, n in enumerate(nodes)}

    labels = [n.replace('src_', '').replace('dst_', '').replace(
        'd', '').replace('t', '') for n in nodes]
    customdata = addNetworkOwners(sandf, labels)

    so, ta, vals, tuples = [], [], [], []
    for index, row in sandf.iterrows():
        typ = f"t{row['jumpedFrom']}"
        diff = f"d{row['diff']}"
        ssite = f"src_{row['src_site']}"
        dsite = f"dst_{row['dest_site']}"
        cnt = row['count']
        if (ssite, typ) not in tuples:
            so.append(nodesdict[ssite])
            ta.append(nodesdict[typ])
            vals.append(cnt)
            tuples.append((ssite, typ))

        if (typ, diff) not in tuples:
            so.append(nodesdict[typ])
            ta.append(nodesdict[diff])
            vals.append(cnt)
            tuples.append((typ, diff))

        if (diff, dsite) not in tuples:
            so.append(nodesdict[diff])
            ta.append(nodesdict[dsite])
            vals.append(cnt)
            tuples.append((diff, dsite))

    return labels, so, ta, vals, customdata


# '''Creates a Sankey diagram'''
def buildSankey(sitesState, asnState, df):
    if len(sitesState) > 0 and len(asnState) > 0:
        df = df[((df['src_site'].isin(sitesState)) |
                (df['dest_site'].isin(sitesState))) & ((df['jumpedFrom'].isin(asnState)) |
                (df['diff'].isin(asnState)))]

    elif len(sitesState) > 0:
        df = df[(df['src_site'].isin(sitesState)) |
                (df['dest_site'].isin(sitesState))]
    elif len(asnState) > 0:
        df = df[(df['jumpedFrom'].isin(asnState)) |
                (df['diff'].isin(asnState))]

    labels, sources, targets, vals, customdata = data4Sankey(df)

    if len(df) > 0:
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="grey", width=0.5),
                label=labels,
                customdata=customdata,
                hovertemplate='%{customdata}<extra></extra>',
                color="rgb(4, 111, 137)"
            ),
            link=dict(
                # indices correspond to labels
                source=sources,
                target=targets,
                value=vals
            ))])

        for x_coordinate, column_name in enumerate(["Source site", "Previously used ASN", "New ASN", "Desination site"]):
            fig.add_annotation(
                x=x_coordinate,
                y=1.15,
                xref="x",
                yref="paper",
                text=column_name,
                showarrow=False,
                font=dict(
                    size=16,
                ),
                #   align="center",
            )

        fig.update_layout(
            height=600,
            # title_text=f"Short term path deviations between sites",
            xaxis={
                'showgrid': False,  # thin lines in the background
                'zeroline': False,  # thick line at x=0
                'visible': False,  # numbers below
            },
            yaxis={
                'showgrid': False,  # thin lines in the background
                'zeroline': False,  # thick line at x=0
                'visible': False,  # numbers below
            },
            # margin=dict(b=2, l=0, r=0),
            plot_bgcolor='rgba(0,0,0,0)',
            font_size=10)
    else:
        fig = go.Figure(data=[go.Sankey()])
        fig.update_layout(
            annotations=[
                dict(
                    text="No data available for the selected criteria or there was no AS number previuosly used at the position of the new ASN.",
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

