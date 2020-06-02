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
    # Move duplicating endpoints up 0.01
    # Need to remove 'normal' points having only entry exit and not ovelapping
    wo_duplicates = pdagr.drop_duplicates(subset=['pointTpMapX', 'pointTpMapY'], keep=False)
    duplicates_only = pdagr[~pdagr.apply(tuple, 1).isin(wo_duplicates.apply(tuple, 1))]
    duplicates_only = duplicates_only.sort_values(by=['pointTpMapX', 'pointTpMapY', 'name'])
    prev_name = ''
    coords_before = [0.0, 0.0]
    coords_after = [0.0, 0.0]
    # It`s a shame but I couldn`t find a way to vectorise cycle below
    # Still not working as intended. Thinking on moving only not matching names
    for index, row in duplicates_only.iterrows():
        curr_coords = row[['pointTpMapX', 'pointTpMapY']].tolist()
        if coords_before == curr_coords and prev_name != row['name']:
            coords_before = curr_coords
            duplicates_only.at[index, 'pointTpMapY'] = coords_after[1] + 0.005
        elif prev_name == row['name']:
            duplicates_only.at[index, 'pointTpMapY'] = coords_before[1]
        else:
            coords_before = curr_coords
        coords_after[0] = duplicates_only.at[index, 'pointTpMapX']
        coords_after[1] = duplicates_only.at[index, 'pointTpMapY']
        prev_name = row['name']

    pdagr = wo_duplicates.append(duplicates_only)

    # Create data for lines from BZ
    joined = pdagr.merge(pdbz, left_on='fromBzKey', right_on='bzKey')
    lines_from = joined.dropna(subset=['fromBzKey'])
    lines_from = lines_from.rename(columns={'name_x': 'name'})
    # Create data for lines to BZ
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
    ips_list = pdagr[['name', 'pointTpMapX', 'pointTpMapY', 'pointKey']].drop_duplicates()
    # Get list of UGS
    UGS_list = ips_list[ips_list['name'].str.contains('UGS')]
    # Get list of LNG
    LNG_list = ips_list[ips_list['pointKey'].str.contains('LNG')]
    # Remove UGS and  LNG from ips_list
    ips_list = ips_list[~ips_list['name'].str.contains('UGS')]
    ips_list = ips_list[~ips_list['pointKey'].str.contains('LNG')]
    # Work with bokeh

    balance_zones = ColumnDataSource(pdbz)
    ips = ColumnDataSource(ips_list)
    UGS = ColumnDataSource(UGS_list)
    LNG = ColumnDataSource(LNG_list)

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
              line_width=1,
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
    # Plot LNG
    lng_points = p.diamond(x='pointTpMapX',
                          y='pointTpMapY',
                          source=LNG,
                          size=12,
                          fill_color='#2DDBE7',
                          legend_label='Терминалы СПГ')

    hover = HoverTool(renderers=[bzp, icp, ugs_points, lng_points])
    hover.tooltips = [
        ('Label', '@name'),
    ]

    p.add_tools(hover)

    # return json
    return json.dumps(json_item(p, "myplot"))

if __name__ == "__main__":
    plot_ENTSOG_map()