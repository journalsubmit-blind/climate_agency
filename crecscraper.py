import requests
import zipfile
import datetime as dt
import io
import lxml.html as html
import sqlite3


D = dt.date.today() - dt.timedelta(days=2)


def get_html_text():
    r = requests.get(
        "http://www.gpo.gov/fdsys/pkg/CREC-{0}.zip"
        .format(D.strftime("%Y-%m-%d")))
    if r.status_code == 404:
        r.raise_for_status()
    else:
        zip_content = io.BytesIO(r.content)
        zip_list = zipfile.ZipFile(zip_content).namelist()
        zip_read = []
        for i in zip_list:
            if ".htm" in i:
                zip_read.append(zipfile.ZipFile(zip_content).read(str(i)))
        for i in range(len(zip_read)):
            html_data = html.fromstring(zip_read[i]).text_content()
            add_to_db(html_data)


def add_to_db(html_data):
    conn = sqlite3.connect('crec.db')
    cur = conn.cursor()

    cur.execute('''CREATE TABLE if not exists crec
                (id INTEGER PRIMARY KEY, html_data TEXT UNIQUE,
                 UTC DATE)''')

    cur.execute('''
              INSERT or IGNORE INTO crec VALUES
              (?, ?, ?)''', (None, html_data, D))

    conn.commit()
    conn.close()


try:
    get_html_text()
except requests.HTTPError:
    pass
