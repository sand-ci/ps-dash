# from utils.utils import create_heatmap
# import dash
# import plotly.graph_objects as go
# import json

# def update_graph(n_clicks, data=None, testTypeFilter=None, hostFilter = None, dateFrom=None, dateTo=None, siteData=None):
#     if data == None:
#         print("None data for graph given.")
#         return None
#     print(f"testTypeFilter: {testTypeFilter}")
#     print(f"hostFilter: {hostFilter}")
#     # print(f"all_tests_filters: {all_tests_filters}")
#     # print(f"all_hosts_filter: {all_hosts_filter}")
#     print(f"siteData: {siteData}")
#     triggered_id = dash.callback_context.triggered
#     # print(triggered_id)
#     if triggered_id:
#         if triggered_id[0]['prop_id'] in ['test-type-filter.value', 'hostname-filter.value']:
#             filter = triggered_id[0]['value']
#             print(f"Filter: {filter}")
#             site_index = siteData
#         elif triggered_id[0]['prop_id'] == 'historical-data.data':
#             print("Initial variant")
#             placeholder_figure = go.Figure()

#             # Add a centered annotation with the message
#             placeholder_figure.add_annotation(
#                 text="Your graph will appear here after clicking 'Generate Graph'.",
#                 xref="paper", yref="paper",
#                 x=0.5, y=0.5, showarrow=False,
#                 font=dict(size=16, color="gray"),
#                 align="center"
#             )

#             # Remove axes and grid
#             placeholder_figure.update_xaxes(visible=False)
#             placeholder_figure.update_yaxes(visible=False)
#             placeholder_figure.update_layout(
#                 plot_bgcolor="white",
#                 height=400  # Adjust height as needed
#             )

#             # graph_component = dcc.Graph(id="graph-placeholder", figure=)
#             return placeholder_figure, [], [], {}
#         else:
#             print("Else variant")
#             prop_id = triggered_id[0]['prop_id']
#             prop_id_json = prop_id.split('.n_clicks')[0]
#             parsed_id = json.loads(prop_id_json)
#             site_index = parsed_id['index']
#             # alarmsInst = Alarms()
#             # framesAllAlarms, pivotFramesAllAlarms = alarmsInst.loadData(start_date, end_date)
#             # df, pivotFrame = framesAllAlarms['hosts not found'], pivotFramesAllAlarms['hosts not found']
#             print(f"Button clicked: {site_index}")

#     print("Creating the heatmap....")
#     figure, allTestsFilter, allHostsFilter, selectedSite = create_heatmap(data, site_index, dateFrom, dateTo, testTypeFilter, hostFilter)
#     # print(f"testTypeFilter: {testTypeFilter}")
#     # print(f"hostFilter: {hostFilter}")
#     return [figure, allTestsFilter, allHostsFilter, selectedSite]