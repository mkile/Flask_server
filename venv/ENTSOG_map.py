from distutils.command.check import check

import requests
import pandas
import json
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, DataTable, CustomJS, CheckboxGroup, TableColumn
from bokeh.layouts import column, row, gridplot
from bokeh.models.tools import HoverTool
from bokeh.embed import json_item
from scipy.spatial import ConvexHull
import random


def prepare_BZ_outlines(full_points_list):
    bz_list = full_points_list['toBzKey']
    bz_list.append(full_points_list['fromBzKey'].rename('toBzKey'))
    bz_list = bz_list.drop_duplicates().dropna().to_list()
    result = []
    for bz in bz_list:
        bz_points = full_points_list[full_points_list['toBzKey'] == bz]
        bz_points = bz_points.append(full_points_list[full_points_list['fromBzKey'] == bz])
        bz_points = bz_points.drop_duplicates(['pointTpMapX', 'pointTpMapY'])
        xs = bz_points['pointTpMapX'].to_list()
        ys = bz_points['pointTpMapY'].to_list()
        #plot_dataframe_coords(bz_points, bz)

        list_points = [[x, y] for x, y in zip(xs, ys)]
        if len(bz_points) >= 2:
            try:
                hull = ConvexHull(list_points)
                hull = list(hull.points[hull.vertices])
                hullx = [p[0] for p in hull]
                hully = [p[1] for p in hull]
                result.append([hullx, hully, bz])
                #plot_list_coords(list_points, result[-1], bz)
            except Exception as E:
                print(E)
    return result


def plot_list_coords(sc, dc, name):
    # Debug function
    from bokeh.plotting import figure
    from bokeh.resources import CDN
    from bokeh.embed import file_html

    px = [p[0] for p in sc]
    py = [p[1] for p in sc]

    ox = [p for p in dc[0]]
    oy = [p for p in dc[1]]


    p = figure()
    p.diamond(px, py, size=12, fill_color='#2DDBE7')

    p.patch(ox, oy, alpha=0.5)

    html = file_html(p, CDN, name)
    file = open('.\\html_from_lists\\' + name + '.html', 'w')
    file.write(html)
    file.close()


def plot_dataframe_coords(coords, name):
    # Debug function
    from bokeh.plotting import figure
    from bokeh.resources import CDN
    from bokeh.embed import file_html

    p = figure()
    data = ColumnDataSource(coords)
    p.diamond(x='pointTpMapX',
                           y='pointTpMapY',
                           source=data,
                           size=12,
                           fill_color='#2DDBE7',
                           legend_label='name')

    hover = HoverTool()
    hover.tooltips = [
        ('Label', '@name'),
    ]
    p.add_tools(hover)

    html = file_html(p, CDN, name)
    file = open('.\\html\\' + name + '.html', 'w')
    file.write(html)
    file.close()


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
    # Make balance_zones_outline
    pdbz_temp = pdbz.rename(columns={'tpMapX': 'pointTpMapX', 'tpMapY': 'pointTpMapY', 'bzKey': 'fromBzKey'})
    outlines = prepare_BZ_outlines(pdagr.append(pdbz_temp[['name', 'pointTpMapX', 'pointTpMapY', 'fromBzKey']]))
    del pdbz_temp
    # Move duplicating endpoints up 0.005
    # Need to remove 'normal' points having only entry exit and not ovelapping
    wo_duplicates = pdagr.drop_duplicates(subset=['pointTpMapX', 'pointTpMapY'], keep=False)
    duplicates_only = pdagr[~pdagr.apply(tuple, 1).isin(wo_duplicates.apply(tuple, 1))]
    duplicates_only = duplicates_only.sort_values(by=['pointTpMapX', 'pointTpMapY', 'name'])
    prev_name = ''
    coords_before = [0.0, 0.0]
    coords_after = [0.0, 0.0]
    # Shift points occupying same location
    # It`s a shame but I couldn`t find a way to vectorise cycle below
    for index, row in duplicates_only.iterrows():
        curr_coords = row[['pointTpMapX', 'pointTpMapY']].tolist()
        if coords_before == curr_coords and prev_name != row['name']:
            coords_before = curr_coords
            duplicates_only.at[index, 'pointTpMapY'] = coords_after[1] + 0.005
        elif prev_name == row['name']:
            duplicates_only.at[index, 'pointTpMapY'] = coords_after[1]
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

    p = figure(output_backend="webgl")
    p.sizing_mode = 'scale_width'

    # Create polylines connecting all points going to and from balance zones to show it`s area
    template = """if (cb_obj.active.includes({num})){{{obj}.visible = true}}
                    else {{{obj}.visible = false}}
                    """
    l = []
    args = {}
    code = ''
    num = 0
    r = lambda: random.randint(0, 255)
    bz_list = []
    for bz_outline_x, bz_outline_y, bz in outlines:
        bz_name = 'bz' + str(num)
        bz_list.append(bz) # bz
        l.append(p.patch(bz_outline_x, bz_outline_y, alpha=0.5, fill_color='#%02X%02X%02X' % (r(),r(),r())))
        code += template.format(num=num, obj=bz_name)
        args[bz_name] = l[-1]
        num += 1
    checkboxes = CheckboxGroup(labels=bz_list, active=list(range(len(bz_list))))
    checkboxes.sizing_mode = 'fixed'
    checkboxes.width = 300
    callback = CustomJS(code=code, args=args)
    checkboxes.js_on_click(callback)

    # Testing switch on/off for bz
    # checkbox = CheckboxGroup(labels=['test'], active=[0])
    # callback = CustomJS(code=template.format(num=0, obj='test'), args={'test':l[0]})
    # checkbox.js_on_click(callback)

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

    layout = gridplot(children=[p, checkboxes], ncols=2, sizing_mode='scale_both')

    # return json
    return json.dumps(json_item(layout, "myplot"))

def plot_ENTSOG_table():
    bz = requests.get('https://transparency.entsog.eu/api/v1/balancingzones?limit=-1')
    agr_ic = requests.get('https://transparency.entsog.eu/api/v1/Interconnections?limit=-1')
    pdbz = pandas.DataFrame(json.loads(bz.text)['balancingzones'])
    pdbz = pdbz.drop_duplicates()

    pdagr = pandas.DataFrame(json.loads(agr_ic.text)['Interconnections'])
    pdagr = pdagr.drop_duplicates()
    # Work with bokeh

    balance_zones = ColumnDataSource(pdbz)
    points = ColumnDataSource(pdagr)

    p = figure()
    p.sizing_mode = 'scale_width'

    bzcolumns = [
        TableColumn(field='bzKey', title='Ключ балансовой зоны'),
        TableColumn(field='bzLabel', title='Наименование балансовой зоны'),
        TableColumn(field='bzLabelLong', title='Описание балансовой зоны'),
    ]
    bzdatatable = DataTable(source=balance_zones, columns=bzcolumns)

    pdcolumns = [
        TableColumn(field='pointKey', title='Ключ пункта'),
        TableColumn(field='pointLabel', title='Метка пункта'),
        TableColumn(field='fromBzLabel', title='В пункт поступает из БЗ'),
        TableColumn(field='fromPointKey', title='В пункт поступает из пункта'),
        TableColumn(field='toBzLabel', title='Из пункта поступает из БЗ'),
        TableColumn(field='toPointKey', title='Из пункта поступает в пункта'),
    ]
    pdagrdatatable = DataTable(source=points, columns=pdcolumns)
    
    layout = row(bzdatatable, pdagrdatatable)

    # return json
    return json.dumps(json_item(layout, "mytable"))

if __name__ == "__main__":
    plot_ENTSOG_map()