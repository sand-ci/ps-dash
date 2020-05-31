import dash
import dash_table
import dash_daq as daq
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc

def GenerateLayout(fig):
    layout =  dcc.Loading(className='loading-component1', id="loader11", color="#b3aaaa", fullscreen=True,
                          children=dcc.Graph(figure=fig, style={
                                                            'height': '100vh',
                                                            'width': '100vw'
                                                        }
                                            )
                         )
    return layout