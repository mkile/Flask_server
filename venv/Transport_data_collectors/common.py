import pandas

def filter_df(data, filter, field):
#filter data by filter and return needed field
    result = data.loc[data['date'] == filter, field]
    return result.iat[0]

def add_html_line(textstring):
    return textstring + '<br>'

def add_html_link(textstring):
    return '<a href="' + textstring + '">' + textstring + '</a><br>'

def turn_date(date):
    return date[8:] + '.' + date[5:7] + '.' + date[:4]