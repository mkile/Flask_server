"""
Central module initiating Flask server and updating necessary data, when needed.
"""
import atexit
from datetime import datetime
from os import path, urandom
from sqlite3 import connect
from time import sleep

from apscheduler.schedulers.background import BackgroundScheduler
from bokeh.resources import CDN
from flask import Flask, render_template, url_for, send_from_directory
from flask import redirect, request, session, make_response
from waitress import serve
from werkzeug.exceptions import HTTPException

from dataprocessing.db_works import check_n_create_data_tables
# from dataprocessing.email_sender import process_message
from dataprocessing.entsog import get_ENTSOG_vr_data
from dataprocessing.entsog_map import plot_ENTSOG_map, plot_ENTSOG_table, create_data_table
from dataprocessing.fgsz import get_FGSZ_vr_data
from dataprocessing.update_checker import collect_and_compare_data, get_updated_data

app = Flask(__name__)
path_to_data = './data/'
data_db_name = 'data.db'
settings_db_name = 'settings.db'


def get_updated():
    return datetime.now().strftime('%d.%m.%Y %H:%M:%S')


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(path.join(app.root_path, '../venv/static'),
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
    sleep(2)
    if request.method == 'POST':
        default_login = load_settings('default_login')
        if isinstance(default_login, tuple):
            error = str(default_login)
        else:
            if request.form['username'] != default_login:
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


@app.route('/changed_data')
def changed_data():
    if not session.get('logged_in'):
        return render_template('login.html')
    return render_template('changed_data.html', resources=CDN.render(),
                           last_updated=load_settings('last_check_date'))


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
            if data[2] == '#error#':
                return render_template('data.html',
                                       title='Данные по виртуальному реверсу с сайта FGSZ',
                                       vr_data=data,
                                       updated=get_updated(),
                                       need_dynamic=True,
                                       max_date=today,
                                       start_date_html=today,
                                       end_date_html=today)
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


@app.route('/updated_data')
def entsog_show_updated_data():
    if not session.get('logged_in'):
        return render_template('login.html')
    updated_data = get_updated_data(path_to_data + data_db_name)
    if isinstance(updated_data, tuple):
        return '<br> Ошибка: ' + updated_data[0] + '<br>' + updated_data[1]
    else:
        return create_data_table(updated_data)


@app.errorhandler(404)
def not_found(error):
    sleep(2)
    return render_template('success.html', result_var='Ошибка, страница не найдена ' + str(error))


@app.errorhandler(HTTPException)
def not_found(error):
    sleep(2)
    return render_template('success.html', result_var='Ошибка сервера ' + str(error))


def check_time_and_send_email():
    # Procedure for checking current time and sending email if it`s over 17:00
    now = datetime.now()
    last_send_date = load_settings('last_send_date')
    if isinstance(last_send_date, tuple):
        print(last_send_date)
        return
    if last_send_date != str(now.strftime('%d.%m.%Y')) and now.hour >= 17:
        message = get_ENTSOG_vr_data(load_settings('points'), email=True)
        message += load_settings('message_ps')
        if len(message) > 0:
            if process_message(message, load_settings('sender_email'), load_settings('reciever_email')):
                save_settings('last_send_date', str(now.strftime('%d.%m.%Y')))


def load_settings(value):
    # Procedure for loading data from database
    # DB contains table data with two text fields: parameter and value
    try:
        conn = connect(path_to_data + settings_db_name)
        c = conn.cursor()
        c.execute("select value from settings where parameter='{}'".format(value))
        result = c.fetchall()
        conn.close()
    except Exception as E:
        return 'Error loading data', E
    if len(result) == 1:
        return result[0][0]
    else:
        result = [a[0] for a in result]
        return result


def save_settings(name, value):
    # Procedure for saving setting to database
    conn = connect(path_to_data + settings_db_name)
    c = conn.cursor()
    c.execute("delete from settings where parameter='{}'".format(name))
    conn.commit()
    if isinstance(value, list):
        for val_element in value:
            c.execute("insert into settings (parameter, value) values ('{}', '{}')".format(name, val_element))
    else:
        c.execute("insert into settings (parameter, value) values ('{}', '{}')".format(name, value))
    conn.commit()
    conn.close()


def run_update_checker():
    # Check for updated data and save it to separate table
    now = datetime.now()
    now_str = str(now.strftime('%d.%m.%Y'))
    last_check_date = load_settings('last_check_date')
    if isinstance(last_check_date, tuple):
        print(last_check_date)
        return
    if last_check_date != now_str and now.hour < 11:
        result = collect_and_compare_data(path_to_data + data_db_name, now)
        if isinstance(result, tuple):
            print(result)
        else:
            save_settings('last_check_date', now_str)


app.config['SECRET_KEY'] = urandom(16)
app.config['DEBUG'] = True

if __name__ == '__main__':
    db_check = check_n_create_data_tables(path_to_data, data_db_name)
    if db_check is None:
        # collect_and_compare_data(path_to_aux_db, datetime.now())
        # run_update_checker()
        # app.run(host='0.0.0.0', threaded=True)
        # Background process for sending email at designated time
        scheduler = BackgroundScheduler()
        # scheduler.add_job(func=check_time_and_send_email, trigger='interval', minutes=5)
        scheduler.add_job(func=run_update_checker, trigger='interval', hours=1)
        scheduler.start()
        # serve app
        serve(app, host='0.0.0.0', port=5000)
        atexit.register(lambda: scheduler.shutdown())
    else:
        print(db_check)
