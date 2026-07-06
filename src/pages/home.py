import numpy as np
import dash
from dash import dash_table, dcc, html
import dash_bootstrap_components as dbc

from dash.dependencies import Input, Output, State

import plotly.express as px

import pandas as pd
from datetime import date

from model.Alarms import Alarms
import utils.helpers as hp
from utils.helpers import timer
from utils.parquet import Parquet
import pycountry
from utils.utils import buildMap, generateStatusTable, explainStatuses
# import psconfig.api


@timer
def builMap(df):
    return buildMap(df)


@timer
def generate_status_table(alarmCnt):
    return generateStatusTable(alarmCnt)


def get_country_code(country_name):
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_2
    except LookupError:
        return ''


def total_number_of_alarms(sitesDf):
    metaDf = pq.readFile('parquet/raw/metaDf.parquet')
    if sitesDf.empty:
        highest_site = None
        highest_site_alarms = 0
        highest_country = None
        highest_country_alarms = 0
    else:
        sitesDf = pd.merge(sitesDf, metaDf[['lat', 'lon', 'country']], on=['lat', 'lon'], how='left').drop_duplicates()
        site_totals = sitesDf.groupby('site')[['Infrastructure', 'Network', 'Other']].sum()

        highest_site = site_totals.sum(axis=1).idxmax()
        highest_site_alarms = site_totals.sum(axis=1).max()

        country_totals = sitesDf.groupby('country')[['Infrastructure', 'Network', 'Other']].sum()
        highest_country = country_totals.sum(axis=1).idxmax()
        highest_country_alarms = country_totals.sum(axis=1).max()

    status = {'critical': '🔴', 'warning': '🟡', 'ok': '🟢', 'unknown': '⚪'}
    if {'Status', 'site'}.issubset(sitesDf.columns):
        status_count = sitesDf[['Status', 'site']].groupby('Status').count().to_dict()['site']
    else:
        status_count = {}
    for s, icon in status.items():
        if icon not in status_count:
            status_count[icon] = 0
    status_explanations = {
        'critical': "'bandwidth decreased from/to multiple sites' observed",
        'warning': ("'ASN path anomalies per site (to several destination or from several sources)'\n"
                    "or 'high delay from/to multiple sites'\nor 'high packet loss on multiple links' observed"),
        'ok': "no alarms",
        'unknown': "other alarms",
    }

    status_box = html.Div([
        html.H6('Status of all sites · past 48 hours',
                className='text-center text-muted pt-3 pb-2 mb-2',
                style={'font-size': '0.78rem', 'text-transform': 'uppercase',
                       'letter-spacing': '0.06em', 'border-bottom': '1px solid #dee2e6'}),
        html.Div([
            html.Div([
                html.Div(className=f'status-bar status-bar-{s} me-2'),
                html.Div([
                    html.P(s, className='text-muted mb-0', style={'font-size': '1rem'}),
                    html.H3(f'{status_count[icon]}', className='mb-0 fw-bold'),
                ]),
                dbc.Tooltip(
                    f"{status_explanations[s]}",
                    target=f"status-cell-{s}",
                    placement="top",
                )
            ], id=f"status-cell-{s}", className='status-cell d-flex align-items-center py-2 px-2')
            for s, icon in status.items()
        ], className='d-flex gap-3 px-3 pb-3'),
    ], className='boxwithshadowhidden mb-2')

    highest_site_label = highest_site or 'No sites'
    highest_country_label = highest_country or 'No countries'
    country_code = get_country_code(sitesDf[sitesDf['site'] == highest_site]['country'].values[0]) if highest_site else ''
    highest_row = dbc.Row([
        dbc.Col([
            html.Div([
                html.H6('Highest alarms from site',
                        className='text-center text-muted pb-1 mb-2',
                        style={'font-size': '0.78rem', 'text-transform': 'uppercase',
                               'letter-spacing': '0.06em', 'border-bottom': '1px solid #dee2e6'}),
                html.H4(f'{highest_site_label} ({country_code}): {highest_site_alarms}',
                        className='fw-bold mb-0', style={'color': '#2d3748'}),
            ], className='boxwithshadow p-3 text-center h-100'),
        ], md=6, sm=12, className='mb-2'),
        dbc.Col([
            html.Div([
                html.H6('Highest alarms from country',
                        className='text-center text-muted pb-1 mb-2',
                        style={'font-size': '0.78rem', 'text-transform': 'uppercase',
                               'letter-spacing': '0.06em', 'border-bottom': '1px solid #dee2e6'}),
                html.H4(f'{highest_country_label}: {highest_country_alarms}',
                        className='fw-bold mb-0', style={'color': '#2d3748'}),
            ], className='boxwithshadow p-3 text-center h-100'),
        ], md=6, sm=12, className='mb-2'),
    ], className='mb-2')

    return status_box, highest_row
 

dash.register_page(__name__, path='/')

pq = Parquet()
alarmsInst = Alarms()


def layout(**other_unknown_query_strings):
    dateFrom, dateTo = hp.defaultTimeRange(2)
    now = hp.defaultTimeRange(days=2, datesOnly=True)
    alarmCnt = pq.readFile('parquet/alarmsGrouped.parquet')
    statusTable, sitesDf = generateStatusTable(alarmCnt)
    print("Period:", dateFrom, " - ", dateTo)
    print(f'Number of alarms: {len(alarmCnt)}')

    status_box, highest_row = total_number_of_alarms(sitesDf)

    return dbc.Container([
        # Top section: map + bar on the left, status info on the right
        dbc.Row([
            dbc.Col([
                html.Div(
                    dcc.Graph(figure=buildMap(sitesDf), id='site-map',
                              responsive=True, className='cls-site-map'),
                    className='boxwithshadow mb-2 p-2',
                ),
                html.Div(
                    dcc.Loading(
                        html.Div(id="alarms-stacked-bar"),
                        style={'height': '1rem'}, color='#00245A'
                    ),
                    className='boxwithshadow flex-grow-1 p-2',
                ),
            ], lg=6, md=12, className='d-flex flex-column'),

            dbc.Col([
                status_box,
                highest_row,
                html.Div([
                    html.Div(children=statusTable, id='site-status', className='status-table-cls'),
                    html.Div([
                        dbc.Button(
                            "How was the status determined?",
                            id="how-status-collapse-button",
                            className="mb-3",
                            color="secondary",
                            n_clicks=0,
                        ),
                        dbc.Modal([
                            dbc.ModalHeader(dbc.ModalTitle("How was the status determined?")),
                            dbc.ModalBody(id="how-status-modal-body"),
                            dbc.ModalFooter(
                                dbc.Button("Close", id="close-how-status-modal", className="ml-auto", n_clicks=0)
                            ),
                        ],
                        id="how-status-modal",
                        size="lg",
                        is_open=False,
                        ),
                    ], className="how-status-div"),
                ], className='boxwithshadow flex-grow-1 p-2'),
            ], lg=6, md=12, className='d-flex flex-column'),
        ], className='g-3 mb-2'),

        # Search section
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col(
                        html.H3([
                            html.I(className="fas fa-search"),
                            " Search the Networking Alarms"
                        ], className="l-h-3"),
                        align="center", md=12, xl=6
                    ),
                    dbc.Col(
                        dcc.DatePickerRange(
                            id='date-picker-range',
                            className='date-picker-range-compact',
                            month_format='M-D-Y',
                            display_format='MM/DD/YYYY',
                            min_date_allowed=date.today() - pd.Timedelta(days=30),
                            initial_visible_month=now[0],
                            start_date=now[0],
                            end_date=now[1]
                        ),
                        md=12, xl=6, className="mb-2 text-right"
                    ),
                ], className='flex-wrap mb-2'),
                dcc.Dropdown(multi=True, id='sites-dropdown',
                             placeholder="Search for a site", className='mb-3'),
                dcc.Dropdown(multi=True, id='events-dropdown',
                             placeholder="Search for an event type", className='mb-3'),
                dbc.Button("Search", id="search-button", color="secondary",
                           style={"width": "100%", "font-size": "1.5em"}),
            ], className='p-3')
        ], className='boxwithshadow mb-2 g-0'),

        # Alarms list
        dbc.Row([
            dbc.Col([
                html.H1("List of alarms", className="text-center mt-1"),
                html.Hr(className="my-2"),
                dcc.Loading(
                    html.Div(id='results-table'),
                    style={'height': '0.5rem'}, color='#00245A'
                )
            ], className='p-3')
        ], className='boxwithshadow mb-2 g-0'),

    ], fluid=True, className='px-3 py-3')
    
@dash.callback(
    [
        Output("sites-dropdown", "options"),
        Output("events-dropdown", "options"),
        Output('alarms-stacked-bar', 'children'),
        Output('results-table', 'children'),
    ],
    [
        Input('search-button', 'n_clicks'),
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
        Input("sites-dropdown", "search_value"),
        Input("sites-dropdown", "value"),
        Input("events-dropdown", "search_value"),
        Input("events-dropdown", "value")
    ],
    State("sites-dropdown", "value"),
    State("events-dropdown", "value")
)
def update_output(n_clicks, start_date, end_date, sites, all, events, allevents, sitesState, eventsState):
    ctx = dash.callback_context
    print('Check date picker')
    print('start:', start_date)
    print('end:', end_date)
    if not ctx.triggered or ctx.triggered[0]['prop_id'].split('.')[0] == 'search-button':
        if not start_date and not end_date:
            start_date, end_date = hp.defaultTimeRange(days=2)
        start_date = pd.Timestamp(start_date).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        end_date = pd.Timestamp(end_date).replace(hour=23, minute=59, second=59, microsecond=0).isoformat()
         
        alarmsInst = Alarms()
        frames, pivotFrames = alarmsInst.loadData(start_date, end_date)

        scntdf = pd.DataFrame()
        for e, df in pivotFrames.items():
            if len(df) > 0:
                df['tag'] = df['tag'].str.upper()
                if 'site' in df.columns:
                    df['site'] = df['site'].str.upper()
                df = df[df['tag'] != ''].groupby('tag')[['id']].count().reset_index().rename(columns={'id': 'cnt', 'tag': 'site'})
                df['event'] = e
                scntdf = pd.concat([scntdf, df])

        no_alarms_msg = html.Div(
            html.P("No alarms in this period", className='text-muted mb-0',
                   style={'font-size': '1rem'}),
            className='text-center py-4'
        )

        if scntdf.empty:
            return [[], [], no_alarms_msg, no_alarms_msg]

        # sites
        graphData = scntdf
        if (sitesState is not None and len(sitesState) > 0):
            graphData = graphData[graphData['site'].isin(sitesState)]

        sites_dropdown_items = []
        for s in sorted(scntdf['site'].unique()):
            if s:
                sites_dropdown_items.append({"label": s.upper(), "value": s.upper()})

        # events
        if eventsState is not None and len(eventsState) > 0:
            graphData = graphData[graphData['event'].isin(eventsState)]

        events_dropdown_items = []
        for e in sorted(scntdf['event'].unique()):
            events_dropdown_items.append({"label": e, "value": e})

        if len(graphData) == 0:
            return [sites_dropdown_items, events_dropdown_items, no_alarms_msg, no_alarms_msg]

        bar_chart = create_bar_chart(graphData)

        dataTables = []
        events = list(pivotFrames.keys()) if not eventsState or events else eventsState
        print("EVENTS")
        print(events)
        for event in sorted(events):
            df = pivotFrames[event]
            if 'site' in df.columns:
                df = df[df['site'].isin(sitesState)] if sitesState is not None and len(sitesState) > 0 else df
            elif 'tag' in df.columns:
                df = df[df['tag'].isin(sitesState)] if sitesState is not None and len(sitesState) > 0 else df

            if len(df) > 0:
                dataTables.append(generate_tables(frames[event], df, event, alarmsInst))

        dataTables = html.Div(dataTables) if dataTables else no_alarms_msg

        return [sites_dropdown_items, events_dropdown_items, dcc.Graph(figure=bar_chart, responsive=True), dataTables]
    else:
        raise dash.exceptions.PreventUpdate 

@dash.callback(
    [
    Output("how-status-modal", "is_open"),
    Output("how-status-modal-body", "children"),
    ],
    [
        Input("how-status-collapse-button", "n_clicks"),
        Input("close-how-status-modal", "n_clicks"),
    ],
    [State("how-status-modal", "is_open")],
)
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        if not is_open:
            catTable, statusExplainedTable = explainStatuses()
            data = dbc.Row([
                dbc.Col(children=[
                    html.H3('Category & Alarm types', className='status-title'),
                    html.Div(statusExplainedTable, className='how-status-table')
                ], lg=12, md=12, sm=12, className='page-cont pr-1 how-status-cont'),
                dbc.Col(children=[
                    html.H3('Status color rules', className='status-title'),
                    html.Div(catTable, className='how-status-table')
                ], lg=12, md=12, sm=12, className='page-cont how-status-cont')
            ], className='pt-1')
            return not is_open, data
        return not is_open, dash.no_update
    return is_open, dash.no_update


def create_bar_chart(graphData):
    # Calculate the total counts for each event type
    # event_totals = graphData.groupby('event')['cnt'].transform('sum')
    # Calculate percentage for each site relative to the event total
    # graphData['percentage'] = (graphData['cnt'] / event_totals) * 100

    graphData['percentage'] = graphData.groupby(['site', 'event'])['cnt'].transform(lambda x: x / x.sum() * 100)

    # Create the bar chart using percentage as the y-axis
    fig = px.bar(
        graphData, 
        x='site', 
        y='percentage', 
        color='event', 
        labels={'percentage': 'Percentage (%)', 'site': '', 'event': 'Event Type'},
        barmode='stack',
        color_discrete_sequence=px.colors.qualitative.Prism
    )

    # Add custom tooltip with original counts
    fig.update_traces(
        hovertemplate="<br>".join([
            "<span style='font-size:15px'>Site: %{x}</span>",
            "<span style='font-size:15px'>Count: %{customdata[0]}</span>",
        ]),
        customdata=graphData[['cnt', 'event']].values
    )

    # Update layout parameters
    fig.update_layout(
        margin=dict(t=20, b=20, l=0, r=0),
        showlegend=True,
        legend_orientation='h',
        legend_title_text='Alarm Type',
        legend=dict(
            x=0,
            y=1.35,
            orientation="h",
            xanchor='left',
            yanchor='top',
            font=dict(
                size=10,
            ),
        ),
        # height=600,
        plot_bgcolor='#fff',
        autosize=True,
        width=None,
        title={
            'y': 0.01,
            'x': 0.95,
            'xanchor': 'right',
            'yanchor': 'bottom'
        },
        xaxis=dict(
            # tickangle=-45,
            automargin=True
        ),
        modebar={
            "orientation": 'v',
        }
    )

    return fig


# '''Takes selected site from the dropdpwn and generates a Dash datatable'''
def generate_tables(frame, unpacked, event, alarmsInst):
    ids = unpacked['id'].values

    dfr = frame[frame.index.isin(ids)]
    dfr = alarmsInst.formatDfValues(dfr, event)
    
    dfr.sort_values('to', ascending=False, inplace=True)
    
    # Replace NaN or empty values with valid defaults
    dfr = dfr.fillna("")  # Replace NaN with an empty string for all columns
    dfr = dfr.astype({col: str for col in dfr.select_dtypes(include=['object', 'category']).columns})  # Ensure all object columns are strings
    drop_columns = {"ASN path anomalies per site": ['sites'], 'destination cannot be reached from any': ['alarm_link'], 'destination cannot be reached from multiple': ['alarm_link'], 'source cannot reach any': ['alarm_link']}
    if event in drop_columns:
        dfr.drop(columns=drop_columns[event], inplace=True)
    dfr.sort_values('to', ascending=False, inplace=True)
    print('Home page,', event, "Number of alarms:", len(dfr))
    try:
        element = html.Div([
            html.H6(event.upper(),
                    className='text-muted pb-2 mb-3 mt-2',
                    style={'font-size': '0.78rem', 'text-transform': 'uppercase',
                           'letter-spacing': '0.06em', 'border-bottom': '1px solid #dee2e6'}),
            dash_table.DataTable(
                data=dfr.to_dict('records'),
                columns=[{"name": i, "id": i, "presentation": "markdown"} for i in dfr.columns],
                markdown_options={"html": True},
                id=f'search-tbl-{event.replace(" ", "-")}',
                page_current=0,
                page_size=10,
                style_as_list_view=True,
                style_cell={
                    'padding': '10px 14px',
                    'font-size': '13px',
                    'whiteSpace': 'pre-line',
                    'fontFamily': 'inherit',
                    'color': '#2d3748',
                },
                style_header={
                    'backgroundColor': '#f8f9fa',
                    'fontWeight': '600',
                    'fontSize': '11px',
                    'textTransform': 'uppercase',
                    'letterSpacing': '0.05em',
                    'color': '#6c757d',
                    'borderBottom': '2px solid #dee2e6',
                },
                style_data={
                    'height': 'auto',
                    'lineHeight': '20px',
                    'overflowX': 'auto',
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': '#f8f9fc',
                    },
                ],
                style_table={
                    'overflowY': 'auto',
                    'overflowX': 'auto',
                    'border': '1px solid #dee2e6',
                    'borderRadius': '0.5rem',
                },
                filter_action="native",
                filter_options={"case": "insensitive"},
                sort_action="native",
            ),
        ], className='single-table')
        return element
    except Exception as e:
        print('dash_table.DataTable expects each cell to contain a string, number, or boolean value', e)
        return html.Div()
        
def count_unique_not_found_hosts(df, category):
    """
    The function helps to count unique hosts among \
    different categories from the Alarms to count the general statistics \
    about perfSonar missing tests in Elasticsearch.
    """
    missing_hosts = df.groupby("site")["hosts_not_found"].apply(lambda x: set(
            host for d in x 
            if isinstance(d, dict) and isinstance(d.get(category), (list, set, np.ndarray))
            for host in (d[category].tolist() if isinstance(d[category], np.ndarray) else d[category])
        ))
    all_missing_hosts = set().union(*missing_hosts.dropna())
    # print(all_missing_hosts)
    return (
        len(all_missing_hosts)
    )

def build_pie_chart(stats, test_type):
    """
    The function builds pie chart with general
    statistics about data availability in Elasticsearch.
    """
    title = test_type
    if test_type == 'owd':
        title = 'latency'
    
    part, total = stats[test_type]
    percentage = (part / total) * 100

    labels = ['Not Found', 'Found']
    values = [part, total - part]

    fig = px.pie(
        names=labels,
        values=values,
        hole=0.4,
        color=labels,
        color_discrete_map={'Not Found': '#00245a', 'Found': '#69c4c4'} 
    )

    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, t=20, b=20),
        title={
            'text': title.upper(),  
            'y': 0.95,  
            'x': 0.05,
            'xanchor': 'left',
            'yanchor': 'top',
            'font': {'size': 12, 'color': '#00245a'}  
        },
        showlegend=False,
        template='plotly_white',
        annotations=[
            {
                'text': f'{(100-percentage):.1f}%',
                'x': 0.5,
                'y': 0.5,
                'font_size': 15,
                'showarrow': False
            }
        ]
    )

    fig.update_traces(
        marker=dict(
            line=dict(color='#ffffff', width=2)  # Add a white border to the slices
        )
    )

    return fig
