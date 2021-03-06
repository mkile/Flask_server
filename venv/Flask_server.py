"""
Central module initiating Flask server and updating necessary data, when needed.
"""
from flask import Flask, render_template, url_for, copy_current_request_context, send_from_directory
from flask import redirect, request, session, make_response
from Transport_data_collectors.FGSZ import get_FGSZ_vr_data
from Transport_data_collectors.ENTSOG import get_ENTSOG_vr_data
from ENTSOG_map import plot_ENTSOG_map, plot_ENTSOG_table
from Email_sender import process_message
import os
from time import sleep
import sqlite3

import atexit
from apscheduler.schedulers.background import BackgroundScheduler

from werkzeug.exceptions import HTTPException
from bokeh.resources import CDN

from datetime import datetime
from waitress import serve

app = Flask(__name__)

def get_updated():
    return datetime.now().strftime('%d.%m.%Y %H:%M:%S')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/')
def start_page():
    if not session.get('logged_in'):
        return render_template('login.html')
    return render_template('index.html')

# Route for handling the login page logic
@app.route('/login', methods=['POST', 'GET'])
def login():
    error = None
    sleep(1)
    if request.method == 'POST':
        if request.form['username'] != load_settings('default_login'):
            error = 'Неверные параметры входа.'
        else:
            session['logged_in'] = True
            return redirect(url_for('start_page'))
    return render_template('success.html', result_var=error)

@app.route("/logout")
def logout():
    session['logged_in'] = False
    return render_template('success.html', result_var='Вы вышли из системы.')

@app.route('/entsog')
def entsog_page():
    if not session.get('logged_in'):
        return render_template('login.html')
    return render_template('data.html', title='Данные по виртуальному реверсу из ENTSOG',
                           vr_data=get_ENTSOG_vr_data(load_settings('points')),
                           updated=get_updated(),
                           sent_date=load_settings('last_send_date'))

@app.route('/gis_data')
def entsog_gis():
    if not session.get('logged_in'):
        return render_template('login.html')
    return render_template('gis_data.html', resources=CDN.render())

@app.route('/gis_data_plot')
def entsog_gis_plot():
    if not session.get('logged_in'):
        return render_template('login.html')
    return plot_ENTSOG_map()

@app.route('/gis_data_table')
def entsog_table_plot():
    if not session.get('logged_in'):
        return render_template('login.html')
    return plot_ENTSOG_table()

@app.route('/fgsz', methods=['POST', 'GET'])
def fgsz_page():
    if not session.get('logged_in'):
        return render_template('login.html')
    today = datetime.now().strftime('%Y-%m-%d')
    if request.method == 'GET':
        data = get_FGSZ_vr_data()
        return render_template('data.html',
                               title='Данные по виртуальному реверсу с сайта FGSZ',
                               vr_data=data,
                               updated=get_updated(),
                               need_dynamic=True,
                               max_date=today,
                               start_date_html=today,
                               end_date_html=today)
    else:
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        if 'xls' in request.form['action']:
            data = get_FGSZ_vr_data(start_date, end_date, True)
            resp = make_response(data)
            resp.headers["Content-Disposition"] = "attachment; " \
                                                  "filename=exportdata{}-{}".format(
                start_date, end_date
            )
            resp.headers["Content-Type"] = "application/" \
                                           "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            return resp
        else:
            data = get_FGSZ_vr_data(start_date, end_date)
            return render_template('data.html',
                                   title='Данные с сайта FGSZ за период',
                                   need_dynamic=True,
                                   vr_data=data,
                                   updated=get_updated(),
                                   max_date=today,
                                   start_date_html=start_date,
                                   end_date_html=end_date)

@app.errorhandler(404)
def not_found(error):
    sleep(2)
    return render_template('success.html', result_var='Ошибка, страница не найдена ' + str(error))

@app.errorhandler(HTTPException)
def not_found(error):
    sleep(2)
    return render_template('success.html', result_var='Ошибка сервера ' + str(error))

def check_time_and_send_email():
    now = datetime.now()
    if load_settings('last_send_date') != str(now.strftime('%d.%m.%Y')) and now.hour >= 17:
        message = get_ENTSOG_vr_data(load_settings('points'), email=True)
        message += load_settings('message_ps')
        if len(message) > 0:
            if process_message(message, load_settings('sender_email'), load_settings('reciever_email')):
                save_settings('last_send_date', str(now.strftime('%d.%m.%Y')))

def load_settings(value):
    # Procedure for loading settings
    # DB contains table settings with two text fields: parameter and value
    conn = sqlite3.connect('settings.db')
    c = conn.cursor()
    c.execute("select value from settings where parameter='{}'".format(value))
    result = c.fetchall()
    conn.close()
    if len(result) == 1:
        return result[0][0]
    else:
        result = [a[0] for a in result]
        return result

def save_settings(name, value):
    conn = sqlite3.connect('settings.db')
    c = conn.cursor()
    c.execute("delete from settings where parameter='{}'".format(name))
    conn.commit()
    if isinstance(value, list):
        for val_element in list_value:
            c.execute("insert into settings (parameter, value) values ('{}', '{}')".format(name, val_element))
    else:
        c.execute("insert into settings (parameter, value) values ('{}', '{}')".format(name, value))
    conn.commit()
    conn.close()

app.config['SECRET_KEY'] = os.urandom(16)
app.config['DEBUG'] = True

if __name__ == '__main__':
    #app.run(host='0.0.0.0', threaded=True)
    #Background process for sending email at designated time
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=check_time_and_send_email, trigger='interval', minutes=5)
    scheduler.start()
    #serve app
    serve(app, host='0.0.0.0', port=5000)
    atexit.register(lambda: scheduler.shutdown())
