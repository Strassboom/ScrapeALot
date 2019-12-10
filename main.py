from flask import Flask, redirect, render_template, url_for
from databaseAgent import BaseTool
import os
import shutil
shutil.copy(os.path.abspath("launcher.py"),os.path.abspath("venv\\Lib\\site-packages\\pyppeteer\\launcher.py"))
#from requests_html import HTMLSession, AsyncHTMLSession
import datetime
import shutil
from pyppeteer import launch
import asyncio
from lxml import html
import arrow
import time
import atexit
import pygal

from apscheduler.schedulers.background import BackgroundScheduler
#from flask_apscheduler import APScheduler

app = Flask(__name__)


# class ASYNCHTMLSessionFixed copied from:
# Hosted in requests-html issue 293 https://github.com/psf/requests-html/issues/293
# Posted at https://github.com/psf/requests-html/issues/293#issuecomment-536320351
# Provided by User piercefreeman https://github.com/piercefreeman
# class AsyncHTMLSessionFixed(AsyncHTMLSession):
#     """
#     pip3 install websockets==6.0 --force-reinstall
#     """
#     def __init__(self, **kwargs):
#         super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
#         self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])

#     @property
#     async def browser(self):
#         if not hasattr(self, "_browser"):
#             self._browser = await launch(ignoreHTTPSErrors=not(self.verify), headless=True, handleSIGINT=False, handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

#         return self._browser

def tool():
    loop = asyncio.new_event_loop()
    rows = loop.run_until_complete(getAllData())
    app.pm.insertTableRows(f"{app.pm.client.project}.{app.pm.dataset_id}.EntryInfo",rows)


jobTimeout = 60
appTimeout = jobTimeout - 10
    
# async def getTerminals(session=""):
#     badwords = ["Terminal","Parking"]
#     if session == "":
#         session = AsyncHTMLSessionFixed()
#     root = await session.get("https://www.laguardiaairport.com/to-from-airport/parking",timeout=6)
#     await root.html.arender()
#     allTerminals = root.html.xpath('.//div[@id="parkingContent"]/div/div[contains(@class,"term-row")]')
#     getName = lambda row: "".join([word for word in row.xpath('.//div[@class="tp-h-mod"]')[0].text.split(' ') if word not in badwords])
#     rows = []
#     for index,row in enumerate(allTerminals):
#         terminalName = getName(row)
#         rows.append([index+1,terminalName])
#     session.close()
#     return rows

async def getAllData(session=""):
    badwords = ["Terminal","Parking"]
    browser = await launch(handleSIGINT=False,handleSIGTERM=False,handleSIGHUP=False)
    page = await browser.newPage()
    rows = []
    timestamp = str(arrow.utcnow().timestamp)
    await page.goto('https://www.laguardiaairport.com/to-from-airport/parking')
    wholeThing = await page.querySelectorAll('.term-row')
    rows = []
    for thing in wholeThing:
        name = await thing.xpath(".//div[@class='tp-h-mod']")
        nameVar = name[0]
        name = await page.evaluate("(nameVar) => nameVar.textContent",nameVar)
        name = "".join([word for word in name.split(' ') if word not in badwords])
        total = await thing.xpath('.//div[@class="terminal-percentage"]/span')
        totalVar = total[0]
        total = await page.evaluate("(totalVar) => totalVar.textContent",totalVar)
        total = float(total.strip().split('%')[0])/100.0
        rows.append([timestamp,name,total])
    await browser.close()
    del browser
    return rows

@app.route('/')
def home():
    return redirect(url_for('trender'))

@app.route('/beans')
def beans():
    rows = sorted(app.pm.getTableData("EntryInfo"),key=lambda x:ord(x[1][0]))
    return render_template("parkinginfo.html",rows=rows)

@app.route('/trender')
def trender():
    line_chart = pygal.Line(fill=True)
    rows = sorted(app.pm.getAll(),key=lambda x:x[0].format("YYYY MM DD HH mm ss ZZ"))
    timestampMin = arrow.get(rows[0][0],"YYYY MM DD HH mm ss ZZ")
    timestampMax = arrow.get(rows[-1][0],"YYYY MM DD HH mm ss ZZ")
    line_chart.title = f'Terminal Percentage Full after {timestampMax.humanize(timestampMin, only_distance=True, granularity="minute")}'
    line_chart.x_labels = map(str, arrow.Arrow.range("minute",timestampMin,timestampMax))#range(tsMin,tsMax))
    terminals = set([row[1] for row in rows])
    data = {}
    for terminal in terminals:
        data[terminal] = []
    for row in rows:
        data[row[1]].append(row[2])
    for item in sorted([a for a in data.keys()],key=lambda v:ord(v[0])):
        line_chart.add(item,data[item])
        print(item,data[item])
    return render_template("aggregate.html",chart=line_chart.render_data_uri(),appTimeout=appTimeout)

if __name__ == "__main__":
    app.pm = BaseTool()
    tool()
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(func=tool, trigger="interval", seconds=jobTimeout)
    scheduler.start()
    app.run(host='127.0.0.1', port=8080)
    # atexit.register(lambda: scheduler.shutdown())