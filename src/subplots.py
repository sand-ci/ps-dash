from plotly.subplots import make_subplots
import plotly as py
import plotly.graph_objects as go
def PacketlossDelaySubplots(data, title):
    ipv6df = data['ipv6df']
    ipv4df = data['ipv4df']
    pl_symmetry = data['pl_symmetry']
    owd_symmetry = data['owd_symmetry']

    ntp, l1, l2 = [], [], []
    for n in data['ntp']:
        l1.append(list(n.keys())[0])
        l2.append(list(n.values())[0])
    ntp.append(l1)
    ntp.append(l2)

    def reverseSymOrder(data):
        s, d, v = [], [], []
        for key, val in data.items():
            s.append(key[0])
            d.append(key[1])
            v.append(val)
        return [s, d, v]

    colors = {'ipv6': '#1985a1',
          'ipv4': '#e9ce2c',
          'other': '#f2f2f2',
          'dark_font': '#292F36',
          'light_font': 'white'}


    fig = make_subplots(rows=5, cols=3,
                        column_widths=[0.3, 0.4, 0.3],
                        specs=[[{"type": "table"}, {'rowspan': 2}, {"type": "xy", 'rowspan': 2}],
                               [{"type": "table"}, None, None],
                               [{"type": "table"}, {'rowspan': 2}, {"type": "xy", 'rowspan': 2}],
                               [{"type": "table"}, None, None],
                               [{"type": "table"}, None, None]],
                        subplot_titles=("Packet loss pair symmetry", "Packet loss (%)", "Packet loss tests done (%)",
                                        "", "NTP", "Delay mean", "OWD tests done (%)",
                                        "OWD pair symmetry", "", ""), 
                        horizontal_spacing = 0.05, vertical_spacing = 0.075)
    # First row
    fig.add_trace(
        go.Table(
            columnwidth = [130,130,30],
            name='pl_sym_ipv6',
            header=dict(
                values=['src', 'dest', 'IPv6'],
                fill_color='white',
                font=dict(size=15),
                align="center",
                height=30
            ),
            cells=dict(
                values=[k for k in reverseSymOrder(pl_symmetry['ipv6'])],
                fill_color=[[colors['other'], colors['other']],[colors['other'], colors['other']], [colors['ipv6'], colors['ipv6']]],
                align='center',
                font=dict(color=[[colors['dark_font'], colors['dark_font']],
                                 [colors['dark_font'], colors['dark_font']],
                                 [colors['light_font'], colors['light_font']]], size=12),
                height=30
                  )
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(x=ipv6df["ts"],
                   y=ipv6df["packet_loss"]*100,
                   name="ipv6",
                   marker=dict(
                    color=colors['ipv6'],
                    size=20,
                    line=dict(
                        color='MediumPurple',
                        width=2
                        )
                    ),
                   legendgroup='ipv6',
                   showlegend=False),
        row=1, col=2
    )

    fig.add_trace(
        go.Scatter(x=ipv4df["ts"],
                   y=ipv4df["packet_loss"]*100,
                   name="ipv4",
                   marker=dict(
                    color=colors['ipv4'],
                    size=20,
                    line=dict(
                        color='MediumPurple',
                        width=2
                        )
                    ),
                   legendgroup='ipv4',
                   showlegend=False),
        row=1, col=2
    )


    fig.add_trace(go.Bar(x=ipv6df['ts'],
                        y=ipv6df['pl_tests_done'],
                        marker=dict(color=colors['ipv6']),
                        name='ipv6',
                        legendgroup='ipv6',
                        hovertext=ipv6df['pl_doc_count']
                        ), row=1, col=3)
    fig.add_trace(go.Bar(x=ipv4df['ts'],
                        y=ipv4df['pl_tests_done'],
                        name='ipv4',
                        marker=dict(color=colors['ipv4']),
                        legendgroup='ipv4',
                        hovertext=ipv4df['pl_doc_count']
                        ), row=1, col=3)
    
    # Second row
    fig.add_trace(
        go.Table(
            columnwidth = [130,130,30],
            name='pl_sym_ipv4',
            header=dict(
                values=['src', 'dest', 'IPv4'],
                fill_color='white',
                font=dict(size=15),
                align="center",
                height=30
            ),
            cells=dict(
                values=[k for k in reverseSymOrder(pl_symmetry['ipv4'])],
                fill_color=[[colors['other'], colors['other']],
                            [colors['other'], colors['other']],
                            [colors['ipv4'], colors['ipv4']]],
                align='center',
                font=dict(color=[["#292F36", "#292F36"], ["#292F36", "#292F36"], ["#292F36", "#292F36"]], size=12),
                height=30
                  )
        ),
        row=2, col=1
    )
    
    # Third row
    fig.add_trace(
        go.Table(
            columnwidth = [135,35],
            header=dict(
                fill_color='white',
                font=dict(size=14),
                align="center",
                height=30
            ),
            cells=dict(
                values=[n for n in ntp],
                fill_color=colors['other'],
                align='center',
                font=dict(color=colors['dark_font'], size=12),
                height=30)

        ),
        row=3, col=1
    )


    fig.add_trace(
        go.Scatter(x=ipv6df["ts"],
                   y=ipv6df["delay_mean"],
                   marker=dict(
                    color=colors['ipv6'],
                    size=20,
                    line=dict(
                        color=colors['ipv6'],
                        width=2
                        )
                    ),
                   legendgroup='ipv6',
                   showlegend=False),
        row=3, col=2
    )
    fig.add_trace(
        go.Scatter(x=ipv4df["ts"],
                   y=ipv4df["delay_mean"],
                   marker=dict(
                    color=colors['ipv4'],
                    size=20,
                    line=dict(
                        color=colors['ipv4'],
                        width=2
                        )
                    ),
                   legendgroup='ipv4',
                   showlegend=False),
        row=3, col=2
    )
    
    fig.add_trace(go.Bar(x=ipv6df['ts'],
                        y=ipv6df['owd_tests_done'],
                        marker=dict(
                            color=colors['ipv6']),
                        name='ipv6',
                        legendgroup='ipv6',
                        showlegend=False,
                        hovertext=ipv6df['owd_doc_count']
                        ), 

                  row=3, col=3)
    fig.add_trace(go.Bar(x=ipv4df['ts'],
                        y=ipv4df['owd_tests_done'],
                        name='ipv4',
                        marker=dict(
                            color=colors['ipv4']),
                        legendgroup='ipv4',
                        showlegend=False,
                        hovertext=ipv4df['owd_doc_count']
                        ),
                  row=3, col=3)
    # Forth row
    fig.add_trace(
        go.Table(
            columnwidth = [130,130,30],
            name='owd_sym_ipv6',
            header=dict(
                values=['src', 'dest', 'IPv6'],
                fill_color='white',
                font=dict(size=15),
                align="center",
                height=30
            ),
            cells=dict(
                values=[k for k in reverseSymOrder(owd_symmetry['ipv6'])],
                fill_color=[[colors['other'], colors['other']],[colors['other'], colors['other']],
                            [colors['ipv6'], colors['ipv6']]],
                align='center',
                font=dict(color=[[colors['dark_font'], colors['dark_font']],
                                 [colors['dark_font'], colors['dark_font']],
                                 [colors['light_font'], colors['light_font']]], size=12),
                height=30
                  )
        ),
        row=4, col=1
    )
    
    # Fifth row
    fig.add_trace(
        go.Table(
            columnwidth = [130,130,30],
            name='owd_sym_ipv4',
            header=dict(
                values=['src', 'dest', 'IPv4'],
                fill_color='white',
                font=dict(size=15),
                align="center",
                height=30
            ),
            cells=dict(
                values=[k for k in reverseSymOrder(owd_symmetry['ipv4'])],
                fill_color=[[colors['other']],[colors['other']], [colors['ipv4'], colors['ipv4']]],
                align='center',
                font=dict(color=colors['dark_font'], size=12),
                height=30
                  )
        ),
        row=5, col=1
    )

    fig.update_layout(
        showlegend=True,
        title_text=title,
        legend=dict(
            traceorder="normal",
            font=dict(
                family="sans-serif",
                size=12,
            ),
        ),
#         height=1050,
    )
    fig.layout.template = 'plotly_white'
#     py.offline.plot(fig)
    return fig