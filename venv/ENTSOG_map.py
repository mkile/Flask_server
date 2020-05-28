import requests
import pandas
import json
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.models.tools import HoverTool
from bokeh.embed import json_item


def plot_ENTSOG_map():
    bz = requests.get('https://transparency.entsog.eu/api/v1/balancingzones?limit=-1')
    agr_ic = requests.get('https://transparency.entsog.eu/api/v1/Interconnections?limit=-1')
    jsbz = json.loads(bz.text)
    jsbz = jsbz['balancingzones']
    pdbz = pandas.DataFrame(jsbz)
    pdbz = pdbz[['tpMapX', 'tpMapY', 'bzKey', 'bzLabelLong', 'bzTooltip', ]]
    pdbz = pdbz.rename(columns={'bzLabelLong': 'name'})

    jsagr_ic = json.loads(agr_ic.text)
    jsagr_ic = jsagr_ic['Interconnections']
    pdagr = pandas.DataFrame(jsagr_ic)
    pdagr = pdagr.rename(columns={'pointLabel': 'name'})
    pdagr = pdagr[
        ['pointKey',
         'name',
         'pointTpMapX',
         'pointTpMapY',
         'fromBzKey',
         'fromPointKey',
         'toBzKey',
         'toPointKey']]
    pdagr = pdagr.drop_duplicates()

    joined = pdagr.merge(pdbz, left_on='fromBzKey', right_on='bzKey')
    lines_from = joined.dropna(subset=['fromBzKey'])
    lines_from = lines_from.rename(columns={'name_x': 'name'})

    joined = pdagr.merge(pdbz, left_on='toBzKey', right_on='bzKey')
    lines_to = joined.dropna(subset=['toBzKey'])
    lines_to = lines_to.rename(columns={'name_x': 'name'})
    lines_to['name'] = lines_to['name'] + ' -> ' + lines_to['toBzKey']
    lines_to = lines_to[['pointTpMapX',
                         'pointTpMapY',
                         'tpMapX',
                         'tpMapY',
                         'name']]
    # Create list of interconnection points
    ips_list = pdagr[['name', 'pointTpMapX', 'pointTpMapY']].drop_duplicates()
    # Get list of UGS
    UGS_list = ips_list[ips_list['name'].str.contains('UGS')]
    # Remove UGS from ips_list
    ips_list = ips_list[~ips_list['name'].str.contains('UGS')]

    # Work with bokeh

    balance_zones = ColumnDataSource(pdbz)
    ips = ColumnDataSource(ips_list)
    UGS = ColumnDataSource(UGS_list)

    p = figure()
    p.sizing_mode = 'scale_width'

    # lines to bz
    from_x0 = lines_to['pointTpMapX'].tolist()
    from_y0 = lines_to['pointTpMapY'].tolist()
    from_x1 = lines_to['tpMapX'].tolist()
    from_y1 = lines_to['tpMapY'].tolist()

    p.segment(from_x0,
              from_y0,
              from_x1,
              from_y1,
              color="green",
              line_width=4,
              legend_label='Потоки к балансовым зонам')

    # lines from bz
    from_x0 = lines_from['pointTpMapX'].tolist()
    from_y0 = lines_from['pointTpMapY'].tolist()
    from_x1 = lines_from['tpMapX'].tolist()
    from_y1 = lines_from['tpMapY'].tolist()

    p.segment(from_x0,
              from_y0,
              from_x1,
              from_y1,
              color="blue",
              line_width=2,
              line_dash='dashed',
              legend_label='Потоки от балансовых зон')

    # Plot Balancing zones
    bzp = p.circle_x(x='tpMapX',
                    y='tpMapY',
                    source=balance_zones,
                    size=10,
                    line_color='green',
                    fill_color='yellow',
                    legend_label='Балансовые зоны')

    # Plot interconnection points
    icp = p.circle(x='pointTpMapX',
                   y='pointTpMapY',
                   source=ips,
                   size=5,
                   color='red',
                   legend_label='Интерконнекторы')

    # Plot UGS
    ugs_points = p.square(x='pointTpMapX',
                   y='pointTpMapY',
                   source=UGS,
                   size=8,
                   fill_color='#a240a2',
                   legend_label='ПХГ')

    hover = HoverTool(renderers=[bzp, icp, ugs_points])
    hover.tooltips = [
        ('Label', '@name'),
    ]

    p.add_tools(hover)

    # return json
    return json.dumps(json_item(p, "myplot"))

if __name__ == "__main__":
    plot_ENTSOG_map()