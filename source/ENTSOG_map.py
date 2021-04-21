import random
from json import loads, dumps

from bokeh.embed import json_item
from bokeh.layouts import column, row
from bokeh.models import ColumnDataSource, DataTable, CustomJS, CheckboxGroup, TableColumn, Div, Select
from bokeh.models.tools import HoverTool
from bokeh.plotting import figure
from pandas import DataFrame, merge
from requests import get
from scipy.spatial import ConvexHull

bz_link = 'https://transparency.entsog.eu/api/v1/balancingzones?limit=-1'
points_link = 'https://transparency.entsog.eu/api/v1/Interconnections?limit=-1'
operators_link = 'https://transparency.entsog.eu/api/v1/operators'


def prepare_BZ_outlines(full_points_list):
    # Нарисовать контуры описывающие все точки входящие в БЗ
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
        # plot_dataframe_coords(bz_points, bz)

        list_points = [[x, y] for x, y in zip(xs, ys)]
        if len(bz_points) >= 2:
            try:
                hull = ConvexHull(list_points)
                hull = list(hull.points[hull.vertices])
                hullx = [p[0] for p in hull]
                hully = [p[1] for p in hull]
                result.append([hullx, hully, bz])
                # plot_list_coords(list_points, result[-1], bz)
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
    def r():
        return random.randint(0, 255)

    # Построить карту ENTSOG
    bal_zone_data = get(bz_link)
    agr_ic = get(points_link)
    jsbz = loads(bal_zone_data.text)
    jsbz = jsbz['balancingzones']
    pdbz = DataFrame(jsbz)
    pdbz = pdbz[['tpMapX', 'tpMapY', 'bzKey', 'bzLabelLong', 'bzTooltip', ]]
    pdbz = pdbz.rename(columns={'bzLabelLong': 'name'})

    jsagr_ic = loads(agr_ic.text)
    jsagr_ic = jsagr_ic['Interconnections']
    pdagr = DataFrame(jsagr_ic)
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
    for index, row_ in duplicates_only.iterrows():
        curr_coords = row_[['pointTpMapX', 'pointTpMapY']].tolist()
        if coords_before == curr_coords and prev_name != row_['name']:
            coords_before = curr_coords
            duplicates_only.at[index, 'pointTpMapY'] = coords_after[1] + 0.005
        elif prev_name == row_['name']:
            duplicates_only.at[index, 'pointTpMapY'] = coords_after[1]
        else:
            coords_before = curr_coords
        coords_after[0] = duplicates_only.at[index, 'pointTpMapX']
        coords_after[1] = duplicates_only.at[index, 'pointTpMapY']
        prev_name = row_['name']

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
    # Get list of UGSs
    ugs_list = ips_list[ips_list['name'].str.contains('UGS')]
    # Get list of LNGs
    lng_list = ips_list[ips_list['pointKey'].str.contains('LNG')]
    # Remove UGSs and LNGs from ips_list
    ips_list = ips_list[~ips_list['name'].str.contains('UGS')]
    ips_list = ips_list[~ips_list['pointKey'].str.contains('LNG')]

    # Work with bokeh

    balance_zones = ColumnDataSource(pdbz)
    ips = ColumnDataSource(ips_list)
    ugs = ColumnDataSource(ugs_list)
    lng = ColumnDataSource(lng_list)

    p = figure(output_backend="webgl")
    p.sizing_mode = 'scale_width'

    # Create polylines connecting all points going to and from balance zones to show it`s area
    template = """if (cb_obj.active.includes({num})){{{obj}.visible = true}}
                    else {{{obj}.visible = false}}
                    """
    lines = []
    args = {}
    code = ''
    num = 0
    bz_list = []
    for bz_outline_x, bz_outline_y, bal_zone_data in outlines:
        bz_name = 'bz' + str(num)
        bz_list.append(pdbz.loc[pdbz['bzKey'] == bal_zone_data, 'name'].to_string(index=False))  # bz
        lines.append(p.patch(bz_outline_x, bz_outline_y, alpha=0.5, fill_color='#%02X%02X%02X' % (r(), r(), r())))
        code += template.format(num=num, obj=bz_name)
        args[bz_name] = lines[-1]
        num += 1

    # Create title for checkboxes
    boxes_title = Div(text='<h2>Список балансовых зон</h2>')

    # Create checboxes
    checkboxes = CheckboxGroup(labels=bz_list, active=list(range(len(bz_list))))
    callback = CustomJS(code=code, args=args)
    checkboxes.js_on_click(callback)

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
                          source=ugs,
                          size=8,
                          fill_color='#a240a2',
                          legend_label='ПХГ')
    # Plot LNG
    lng_points = p.diamond(x='pointTpMapX',
                           y='pointTpMapY',
                           source=lng,
                           size=12,
                           fill_color='#2DDBE7',
                           legend_label='Терминалы СПГ')

    hover = HoverTool(renderers=[bzp, icp, ugs_points, lng_points])
    hover.tooltips = [
        ('Label', '@name'),
    ]

    p.add_tools(hover)

    layout = row(
        children=[p, column(children=[boxes_title, checkboxes], sizing_mode='scale_both')],
        sizing_mode='scale_height')

    # return json
    return dumps(json_item(layout, "myplot"))


def plot_ENTSOG_table():
    # Создание таблиц БЗ и пунктов Bokeh
    bz = get(bz_link)
    agr_ic = get(points_link)
    pdbz = DataFrame(loads(bz.text)['balancingzones'])
    pdbz = pdbz.drop_duplicates().fillna('-')

    pdagr = DataFrame(loads(agr_ic.text)['Interconnections'])
    pdagr = pdagr.drop_duplicates().fillna('-')
    # Work with bokeh

    balance_zones = ColumnDataSource(pdbz)
    points = ColumnDataSource(pdagr)

    # output_figure = figure()
    # output_figure.sizing_mode = 'scale_width'

    bzcolumns = [
        TableColumn(field='bzKey', title='Ключ балансовой зоны'),
        TableColumn(field='bzLabel', title='Наименование балансовой зоны'),
        TableColumn(field='bzLabelLong', title='Описание балансовой зоны'),
    ]
    bzdatatable = DataTable(source=balance_zones, columns=bzcolumns)
    bz_text = Div(text='<h2>Список балансовых зон</h2>')

    pdcolumns = [
        TableColumn(field='pointKey', title='Ключ пункта'),
        TableColumn(field='pointLabel', title='Метка пункта'),
        TableColumn(field='fromBzLabel', title='Поступает из БЗ'),
        TableColumn(field='fromPointKey', title='Поступает из пункта'),
        TableColumn(field='toBzLabel', title='Поступает в БЗ'),
        TableColumn(field='toPointKey', title='Поступает в пункт'),
    ]
    pdagrdatatable = DataTable(source=points, columns=pdcolumns)
    pd_text = Div(text='<h2>Список пунктов</h2>')

    layout = row(column(children=[bz_text, bzdatatable]),
                 column(children=[pd_text, pdagrdatatable]),
                 sizing_mode='scale_both')

    # return json
    return dumps(json_item(layout, "mytable"))


def load_points_names():
    # Загрузка списков сопоставления пунктов
    try:
        ips = get(points_link)
        ips = loads(ips.text)
        ips = DataFrame(ips['Interconnections'])
    except Exception as error:
        print(error)
        return 'Error loading points names', error
    return ips[['pointLabel', 'pointKey']].drop_duplicates()


def load_operators_names():
    # Загрузка списков сопоставления операторов
    try:
        operators = get(operators_link)
        operators = loads(operators.text)
        operators = DataFrame(operators['operators'])
    except Exception as error:
        print(error)
        return 'Error loading operator names', error
    return operators[['operatorLabel', 'operatorKey']].drop_duplicates()


def create_data_table(pandas_table):
    # Цвета для столбиков
    colors = ["#6c8c30", "#306c8c", "#736648", "#e79e3d"]
    # Подготовим табличку, заменим коды объёктов на их имена
    cols = list(pandas_table.columns)
    cols = cols[-1:] + cols[0:-1]
    pandas_table = pandas_table[cols]
    points = load_points_names()
    loaded_points_names = False
    if not isinstance(points, tuple):
        pandas_table = merge(pandas_table, points, on=['pointKey', 'pointKey'])
        pandas_table = pandas_table.drop(columns=['pointKey'])
        loaded_points_names = True
    operators = load_operators_names()
    if not isinstance(operators, tuple):
        pandas_table = merge(pandas_table, operators, on=['operatorKey', 'operatorKey'])
        pandas_table = pandas_table.drop(columns=['operatorKey'])
    # Создание таблицы Bokeh из таблицы Pandas
    source_table = ColumnDataSource(pandas_table)
    original_source_table = ColumnDataSource(pandas_table)
    column_names = pandas_table.columns.values
    source_columns = [TableColumn(field=cname, title=cname) for cname in column_names]
    agrtable = DataTable(source=source_table, columns=source_columns, height=500, width=1000)
    combined_callback_code = """
    var data = source.data;
    var original_data = original_source.data;
    var date = date_select_obj.value;
    var save_date = save_date_select_obj.value;
    console.log("Date: " + date);
    var indicator = indicator_select_obj.value;
    console.log("Indicator: " + indicator);
    var point = point_select_obj.value;
    console.log("Point: " + point);
    for (var key in original_data) {
        data[key] = [];
        for (var i = 0; i < original_data['periodFrom'].length; ++i) {
            if ((date === "Все" || original_data['periodFrom'][i].match(date)) &&
                (indicator === "Все" || original_data['indicator'][i] === indicator) &&
                (save_date === "Все" || original_data['savedate'][i] === save_date) &&
                (point === "Все" || original_data['pointLabel'][i] === point)) {
                data[key].push(original_data[key][i]);
            }
        }
    }

    source.change.emit();
    target_obj.change.emit();
    """

    # prepare some data for histogram
    table_data = pandas_table[['savedate', 'periodFrom', 'indicator']].drop_duplicates()
    save_dates = table_data['savedate'].drop_duplicates().sort_values()
    dates = table_data['periodFrom'].drop_duplicates().sort_values()
    indicators = table_data['indicator'].drop_duplicates().sort_values().tolist()

    # define filter widgets, without callbacks for now
    save_date_list = ['Все'] + save_dates.tolist()
    save_date_select = Select(title="Дата обновления:", value=save_date_list[0], options=save_date_list)
    date_list = ['Все'] + dates.apply(lambda x: x[:10]).drop_duplicates().tolist()
    date_select = Select(title="Дата:", value=date_list[0], options=date_list)
    indicator_list = ['Все'] + indicators
    indicator_select = Select(title="Индикатор:", value=indicator_list[0], options=indicator_list)
    if loaded_points_names:
        point_list = ['Все'] + pandas_table['pointLabel'].drop_duplicates().sort_values().tolist()
    else:
        point_list = ['Все'] + pandas_table['pointKey'].drop_duplicates().sort_values().tolist()
    point_select = Select(title="Пункт:", value=point_list[0], options=point_list)

    # now define callback objects now that the filter widgets exist
    generic_callback = CustomJS(
        args=dict(source=source_table,
                  original_source=original_source_table,
                  date_select_obj=date_select,
                  save_date_select_obj=save_date_select,
                  indicator_select_obj=indicator_select,
                  point_select_obj=point_select,
                  target_obj=agrtable),
        code=combined_callback_code
    )

    # finally, connect the callbacks to the filter widgets
    save_date_select.js_on_change('value', generic_callback)
    date_select.js_on_change('value', generic_callback)
    indicator_select.js_on_change('value', generic_callback)
    point_select.js_on_change('value', generic_callback)

    # add histogram to visualise changed amounts by date and indicator
    plot_data = pandas_table.groupby(['periodFrom', 'indicator']).size().reset_index(name='Counts')
    plot_data = {x: plot_data.loc[plot_data.indicator == x][['periodFrom', 'Counts']]
                 for x in indicators}
    plot_data = {y: merge(dates, plot_data[y], on=['periodFrom', 'periodFrom'],
                          how='outer')['Counts'].fillna(0).astype('int32').tolist() for y in indicators}
    plot_data['x'] = dates

    plot = figure(x_range=dates, plot_height=400, plot_width=800,
                  title="Количество изменений по суткам",
                  toolbar_location=None, tools="hover", tooltips="$name: @$name")
    plot.vbar_stack(indicators, width=0.9, x='x', source=plot_data,
                    legend_label=indicators, color=colors)
    plot.y_range.start = 0
    plot.xaxis.major_label_orientation = 0.9
    plot.x_range.range_padding = 0.1
    plot.xgrid.grid_line_color = None
    plot.axis.minor_tick_line_color = None
    plot.outline_line_color = None
    plot.legend.location = "top_left"
    plot.legend.orientation = "horizontal"

    layout = column(row(save_date_select, date_select, point_select, indicator_select),
                    agrtable, plot, sizing_mode='stretch_width')
    return dumps(json_item(layout, "mytable"))


if __name__ == "__main__":
    plot_ENTSOG_map()
