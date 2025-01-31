import time
import dash
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc
from dash import html
import dash_loading_spinners
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State

from model.Updater import ParquetUpdater


# cache the data in /parquet.
ParquetUpdater()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css',
                        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.1/css/all.min.css",
                        dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP]

app = Dash(__name__, external_stylesheets=external_stylesheets, 
           external_scripts=[
                {'src': 'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.0/html2canvas.min.js'}
            ],
           suppress_callback_exceptions=True, use_pages=True)


# Kubernetes rediness probe
server = app.server
@server.route('/ready')
def ready():
    return 'OK', 200


nav_item_inline_css = {"color": "white",
                       "margin-right": "1rem",
                       "margin-left": "1rem",
                       "text-decoration": "none",
                       "text-align": "center"}




app.layout = html.Div(children=[
    html.Div(
        id="div-loading",
        children=[
            dash_loading_spinners.Pacman(
                fullscreen=True, 
                id="loading-whole-app"
            )
        ]
    ),
	html.Div(
        className="div-app",
        id="div-app",
        children = [
            dcc.Location(id='change-url', refresh=False),
            dcc.Store(id='store-dropdown'),
            dbc.Row([
                dbc.Col(dbc.Button(
                    "perfSONAR Toolkit Information",
                    class_name="external-button h-100   ",
                    href='https://toolkitinfo.opensciencegrid.org/toolkitinfo/'
                ), lg=3, md=3, sm=6, xs=6),
                dbc.Col(dbc.Button(
                    "Kibana: Packet Loss in OSG/WLCG",
                    class_name="external-button h-100",
                    href='https://atlas-kibana.mwt2.org/s/networking/app/kibana#/dashboard/07a03a80-beda-11e9-96c8-d543436ab024?_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A(from%3Anow-3d%2Cto%3Anow))'
                ), lg=3, md=3, sm=6, xs=6),
                dbc.Col(dbc.Button(
                    "Kibana: Packet Loss Tracking",
                    class_name="external-button h-100",
                    href='https://atlas-kibana.mwt2.org/s/networking/app/dashboards#/view/ab7c4950-5cfa-11ea-bad0-ff3d06e7229e?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-3d,to:now))'
                ), lg=3, md=3, sm=6, xs=6),
                dbc.Col(dbc.Button(
                    "MEPHi Tracer: Traceroute explorer",
                    class_name="external-button h-100",
                    href='https://perfsonar.uc.ssl-hep.org'
                ), lg=3, md=3, sm=6, xs=6),
                # dbc.Col(dbc.Button(
                #     "Alarms description",
                #     class_name="external-button w-100",
                #     href='https://docs.google.com/presentation/d/1QZseDVnhN8ghn6yaSQmPbMzTi53jwUFTr818V_hUjO8/edit#slide=id.p'
                # ), lg=2, md=3, sm=4, xs=12)
            ], class_name="external-links g-0 h-100 flex-wrap"),
            dcc.Location(id='url', refresh=False),
            dbc.Navbar(
                [
                    html.A(
                        dbc.Row(
                            [
                                dbc.Col(html.Img(src=dash.get_asset_url('ps-dash.png'), height="35px"), class_name="logo ml-2 ml-4"),
                                # dbc.Col(dbc.NavbarBrand("Your Brand Name", className="ml-2")),
                            ],
                            align="center", class_name="g-0"
                        ),
                        href="/",
                    ),
                    dbc.NavbarToggler(id="navbar-toggler"),
                    dbc.Collapse(
                        dbc.Nav(
                            [
                               dbc.NavItem(dbc.NavLink("SITES OVERVIEW", href="/",
                                                       id='sites-tab', class_name="nav-item-cls")),
                                # dbc.NavItem(dbc.NavLink("SEARCH ALARMS", href="/search-alarms",
                                #                         id='search-tab', class_name="nav-item-cls"
                                #                         )),
                                dbc.NavItem(dbc.NavLink("EXPLORE PATHS", href="/explore-paths",
                                                        id='paths-tab', class_name="nav-item-cls"
                                                        )),
                                # dbc.NavItem(dbc.NavLink("MAJOR ALARMS", href="/ml-alarms/throughput",
                                #                         id='major-alarms-tab', class_name="nav-item-cls"
                                #                         )),
                            ], class_name="navbar-nav justify-content-around flex-grow-1"
                        ),
                        id="navbar-collapse",
                        navbar=True
                    ),
                ],
                color="#00245a",
                class_name="justify-content-between nav-conatiner",
            ),
            dash.page_container
        ]
    )
], id="component-to-save")


@app.callback(
    [Output('sites-tab', 'active'),
    #  Output('search-tab', 'active'),
     Output('paths-tab', 'active'),
    #  Output('major-alarms-tab', 'active')
     ],
    [Input('url', 'pathname')]
)
def update_active_tab(pathname):
    # url = ''
    # if pathname.startswith('/ml-alarms'):
    #     url = pathname
    return pathname == "/", pathname == "/explore-paths"



@app.callback(
    Output("div-loading", "children"),
    [
        Input("div-app", "loading_state")
    ],
    [
        State("div-loading", "children"),
    ]
)
def hide_loading_after_startup(loading_state, children):
    if children:
        time.sleep(1)
        return None

    raise PreventUpdate


@app.callback(
    Output("navbar-collapse", "is_open"),
    [Input("navbar-toggler", "n_clicks")],
    [State("navbar-collapse", "is_open")],
)
def toggle_navbar_collapse(n, is_open):
    if n:
        return not is_open
    return is_open


app.clientside_callback(
    """
    function(n_clicks){
        if(n_clicks > 0){
            html2canvas(document.getElementById("component-to-save"), {useCORS: true}).then(function (canvas) {
                var anchorTag = document.createElement("a");
                document.body.appendChild(anchorTag);
                anchorTag.download = "download.png";
                anchorTag.href = canvas.toDataURL();
                anchorTag.target = '_blank';
                anchorTag.click();
            });
        }
    }
    """,
    Output('download-image', 'n_clicks'),
    Input('download-image', 'n_clicks')
)


if __name__ == '__main__':
	app.run_server(debug=False, port=8050, host='0.0.0.0')
