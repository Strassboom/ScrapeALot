from flask import Flask, redirect, render_template, url_for
from databaseAgent import BaseTool
from requests_html import HTMLSession, AsyncHTMLSession
import datetime
import pyppeteer
import asyncio
from lxml import html
import arrow

# class ASYNCHTMLSessionFixed copied from:
# Hosted in requests-html issue 293 https://github.com/psf/requests-html/issues/293
# Posted at https://github.com/psf/requests-html/issues/293#issuecomment-536320351
# Provided by User piercefreeman https://github.com/piercefreeman
class AsyncHTMLSessionFixed(AsyncHTMLSession):
    """
    pip3 install websockets==6.0 --force-reinstall
    """
    def __init__(self, **kwargs):
        super(AsyncHTMLSessionFixed, self).__init__(**kwargs)
        self.__browser_args = kwargs.get("browser_args", ["--no-sandbox"])

    @property
    async def browser(self):
        if not hasattr(self, "_browser"):
            self._browser = await pyppeteer.launch(ignoreHTTPSErrors=not(self.verify), headless=True, handleSIGINT=False, handleSIGTERM=False, handleSIGHUP=False, args=self.__browser_args)

        return self._browser


app = Flask(__name__)

    
async def getTerminals():
    badwords = ["Terminal","Parking"]
    session = AsyncHTMLSessionFixed()
    root = await session.get("https://www.laguardiaairport.com/to-from-airport/parking")
    await root.html.arender()
    allTerminals = root.html.xpath('.//div[@id="parkingContent"]/div/div[contains(@class,"term-row")]')
    getName = lambda row: "".join([word for word in row.xpath('.//div[@class="tp-h-mod"]')[0].text.split(' ') if word not in badwords])
    rows = []
    for index,row in enumerate(allTerminals):
        terminalName = getName(row)
        rows.append([index+1,terminalName])
    return rows

async def getAllData():
    badwords = ["Terminal","Parking"]
    timestamp = str(arrow.utcnow().timestamp)
    session = AsyncHTMLSessionFixed()
    root = await session.get("https://www.laguardiaairport.com/to-from-airport/parking")
    await root.html.arender()
    allTerminals = root.html.xpath('.//div[@id="parkingContent"]/div/div[contains(@class,"term-row")]')
    getName = lambda row: "".join([word for word in row.xpath('.//div[@class="tp-h-mod"]')[0].text.split(' ') if word not in badwords])
    getPercent = lambda row: float(row.xpath('.//div[@class="terminal-percentage"]/span')[0].text.strip().split('%')[0])/100.0
    rows = []
    for row in allTerminals:
        terminalName = getName(row)
        percentage = getPercent(row)
        rows.append([timestamp,terminalName,percentage])
    return rows

@app.route('/')
def home():
    app.pm = BaseTool()
    return redirect(url_for('beans'))

@app.route('/beans')
def beans():
    loop = asyncio.new_event_loop()
    rows = loop.run_until_complete(getAllData())
    app.pm.insertTableRows(f"{app.pm.client.project}.{app.pm.dataset_id}.Terminals",rows)
    return render_template("parkinginfo.html",rows=rows)

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080)