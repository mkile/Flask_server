import pandas

def filter_df(data, filter, field):
    # Filter data by filter and return needed field
    result = data.loc[data[field] == filter]
    # TODO:
    # Переписать процедуру и соответствующие процедуры, использующие ее,
    # чтобы работать со всем массивом, а не только с первой строкой
    #return result.iat[0]
    return result

def add_html_line(textstring):
    # Add new html line
    return textstring + '<br>'

def add_html_link(textstring):
    # Add link in html format
    return '<a href="' + textstring + '">' + textstring + '</a><br>'

def turn_date(date):
    # Dirty hack to convert date to necessary format
    return date[8:] + '.' + date[5:7] + '.' + date[:4]