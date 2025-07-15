import dash
from dash import html
import dash_bootstrap_components as dbc

from datetime import datetime
import urllib3

import utils.helpers as hp
from utils.helpers import DATE_FORMAT
from model.Alarms import Alarms
import model.queries as qrs
import pprint
from utils.parquet import Parquet
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

urllib3.disable_warnings()



def title(q=None):
    return f"Destination not reached from multiple sites or any {q}"



def description(q=None):
    return f"Visual represention on a destination not reached from multiple sites or any {q}"



dash.register_page(
    __name__,
    path_template="/reach-destination/<q>",
    title=title,
    description=description,
)



def layout(q=None, **other_unknown_query_strings):
    print(q)
    if not q:
      return html.Div([])
    else:
      if '=' in q:
          params = q.split('&')
          query_params = {param.split('=')[0]: param.split('=')[1] for param in params}
      site = query_params.get('site')
      dtTo = query_params.get('dt_to')
      print("Site:", site)
      print("Date To:", dtTo)
      alarm_desc = qrs.queryUnreachableDestination('destination cannot be reached from multiple', site, dtTo)
      pprint.pprint(alarm_desc)
      all_sites = set()
    
      for entry in alarm_desc['destination cannot be reached from multiple']:
          all_sites.add(entry['site'])
          all_sites.update(entry['cannotBeReachedFrom'])

      all_sites = sorted(all_sites)

      # Step 2: Create an empty DataFrame for the matrix
      unreach_matrix = pd.DataFrame(0, index=all_sites, columns=all_sites)

      # Step 3: Query each site and fill the matrix
      for origin_site in all_sites:
          result = qrs.queryUnreachableDestination('destination cannot be reached from multiple', origin_site, dtTo)
          for entry in result.get('destination cannot be reached from multiple', []):
              destination = entry['site']
              for source in entry['cannotBeReachedFrom']:
                  if source in all_sites and destination in all_sites:
                      unreach_matrix.loc[source, destination] += 1

      # Step 4: Convert to long-form DataFrame for Plotly
      df_long = unreach_matrix.reset_index().melt(id_vars='index')
      df_long.columns = ['Source Site', 'Destination Site', 'Unreachable Count']

      # Step 5: Plotly heatmap
      fig = px.imshow(
          unreach_matrix.values,
          x=unreach_matrix.columns,
          y=unreach_matrix.index,
          color_continuous_scale='Reds',
          labels=dict(x="Destination Site", y="Source Site", color="Unreachable Count"),
          text_auto=True
      )

      fig.update_layout(
          title="Heatmap of Unreachable Site Pairs",
          xaxis_tickangle=45,
          autosize=False,
          width=1000,
          height=800,
      )

      fig.show()
      return html.Div([])

      # alarm = qrs.getAlarm(q)
      # print('URL query:', q)
      # print()
      # print('Alarm content:', alarm)
      # alarmsInst = Alarms()
      # alrmContent = alarm['source']
      # event = alarm['event']
      # kibana_row = loss_delay_kibana(alrmContent, event)
      # return html.Div([
      #         dbc.Row([
      #           dbc.Row([
      #             dbc.Col([
      #               html.H3(event.upper(), className="text-center bold"),
      #               html.H5(alrmContent['to'], className="text-center bold"),
      #             ], lg=2, md=12, className="p-3"),
      #             dbc.Col(
      #                 html.Div(
      #                   [
      #                     dbc.Row([
      #                       dbc.Row([
      #                         html.H1(f"Summary", className="text-left"),
      #                         html.Hr(className="my-2")]),
      #                       dbc.Row([
      #                           html.P(alarmsInst.buildSummary(alarm), className='subtitle'),
      #                         ], justify="start"),
      #                       ])
      #                   ],
      #                 ),
      #             lg=10, md=12, className="p-3")
      #           ], className="boxwithshadow alarm-header pair-details g-0", justify="between", align="center")
      #         ], style={"padding": "0.5% 1.5%"}, className='g-0'),
      #       kibana_row,
      # ], className='mb-5')



