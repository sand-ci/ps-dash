import dash
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP]

app = Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True, use_pages=True)

app.layout = html.Div([
    dcc.Location(id='change-url', refresh=False),
    dcc.Store(id='store-dropdown'),
    dbc.Row([
        dbc.Col(dbc.Button(
            "perfSONAR Toolkit Information",
            className="external-button",
            href='https://toolkitinfo.opensciencegrid.org/toolkitinfo/'
        )),
        dbc.Col(dbc.Button(
            "Kibana: Packet Loss in OSG/WLCG",
            className="external-button",
            href='https://atlas-kibana.mwt2.org/s/networking/app/kibana#/dashboard/07a03a80-beda-11e9-96c8-d543436ab024?_g=(filters%3A!()%2CrefreshInterval%3A(pause%3A!t%2Cvalue%3A0)%2Ctime%3A(from%3Anow-3d%2Cto%3Anow))'
        )),
        dbc.Col(dbc.Button(
            "Kibana: Packet Loss Tracking",
            className="external-button",
            href='https://atlas-kibana.mwt2.org/s/networking/app/dashboards#/view/ab7c4950-5cfa-11ea-bad0-ff3d06e7229e?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-3d,to:now))'
        )),
        dbc.Col(dbc.Button(
            "MEPHi Tracer: Traceroute explorer",
            className="external-button",
            href='https://perfsonar.uc.ssl-hep.org'
        )),
        dbc.Col(dbc.Button(
            "Alarms description",
            className="external-button",
            href='https://docs.google.com/presentation/d/1QZseDVnhN8ghn6yaSQmPbMzTi53jwUFTr818V_hUjO8/edit#slide=id.p'
        ))
    ], className="external-links g-0", justify='center', align="center"),
    dbc.Nav(
        [
            dbc.NavItem(dbc.NavLink(
                html.Img(src=dash.get_asset_url('ps-dash.png'), height="35px"
                        ), disabled=True, href="/", className="logo")),
            dbc.NavItem(dbc.NavLink("SITES OVERVIEW", href="/", id='sites-tab')),
            dbc.NavItem(dbc.NavLink("SEARCH ALARMS", href="/search-alarms", id='search-tab')),
            dbc.NavItem(dbc.NavLink("EXPLORE PATHS", href="/explore-paths", id='paths-tab')),
        ], fill=True, justified=True, id='navbar'
    ),
    # dcc.Loading(
	    dash.page_container
    #     , color='#00245A'
    # )
])


if __name__ == '__main__':
	app.run_server(debug=False, port=8050, host='0.0.0.0')
