from requests_html import HTMLSession
from lxml import html
import arrow


def getPageContent():
    session = HTMLSession()
    root = session.get("https://www.laguardiaairport.com/to-from-airport/parking")
    root.html.render()
    return root.html
    
def getTerminals():
    badwords = ["Terminal","Parking"]
    root = getPageContent()
    allTerminals = root.xpath('.//div[@id="parkingContent"]/div/div[contains(@class,"term-row")]')
    getName = lambda row: "".join([word for word in row.xpath('.//div[@class="tp-h-mod"]')[0].text.split(' ') if word not in badwords])
    rows = []
    for index,row in enumerate(allTerminals):
        terminalName = getName(row)
        rows.append([index+1,terminalName])
    return rows

def getAllData():
    badwords = ["Terminal","Parking"]
    timestamp = str(arrow.utcnow().timestamp)
    root = getPageContent()
    allTerminals = root.xpath('.//div[@id="parkingContent"]/div/div[contains(@class,"term-row")]')
    getName = lambda row: "".join([word for word in row.xpath('.//div[@class="tp-h-mod"]')[0].text.split(' ') if word not in badwords])
    getPercent = lambda row: float(row.xpath('.//div[@class="terminal-percentage"]/span')[0].text.strip().replace('%',''))/100.0
    rows = []
    for row in allTerminals:
        terminalName = getName(row)
        percentage = getPercent(row)
        rows.append([timestamp,terminalName,percentage])
    return rows

if __name__ == "__main__":
    print(getTerminals())