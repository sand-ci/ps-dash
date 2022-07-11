import dash
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', dbc.themes.BOOTSTRAP]

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
        ))
    ], className="external-links", justify='center', align="center"),dbc.Nav(
        [
            dbc.NavItem(dbc.NavLink(
                html.Img(src=dash.get_asset_url('ps-dash.png'), height="35px"
                        ), disabled=True, href="/", className="logo")),
            dbc.NavItem(dbc.NavLink("SITES", href="/", id='sites-tab')),
            # dbc.NavItem(dbc.NavLink("LINKS", href="/nodes", id='nodes-tab')),
            # dbc.NavItem(dbc.NavLink("PLOTS", href="/pairs", id='pairs-tab')),
        ], fill=True, justified=True, id='navbar'
    ),

	dash.page_container
])


if __name__ == '__main__':
	app.run_server(debug=True, port=8050, host='0.0.0.0')