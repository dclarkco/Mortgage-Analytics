
import requests, pandas as pd
import waybackmachine as wm
from bs4 import BeautifulSoup
from datetime import datetime
import sys

sys.path.insert(0, "/Users/dclark/Documents/GitHub/scripts-private/emailFiles")

startDate = '2000-01-01'
endDate = datetime.now()

#cronjob: 30 7 * * 2 python3 /Users/DClark/Documents/scripts/scrapeMarket.py

#nasdaq api key: qmT62ueF51hLvYkbcEQH
# scrapeMarket.py
# Dillon Clark, Godirect Financial 2022
# retreives LIBOR data from WSJ each week

# TODO:
# send email with csv and readable data


URL = ("https://www.wsj.com/market-data/bonds")

def getIngredients(URL):

	headers = {
		'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
		}

	page = requests.get(URL, headers = headers)
	return page

def getLIBOR(URL, toCSV, page):

	page = getIngredients(URL)
	today = wm.fetch(URL, date = endDate)
	print(type(page), type(today.response))

	soup = BeautifulSoup(page.content, "html.parser")

	table = soup.find_all("td", class_="WSJTables--table__cell--2u6629rx")
	index = soup.find_all("th", class_="WSJTables--thead__cell--1Do0eEYL")

	#ids = [tag['id'] for tag in soup.select('div[id]')] # --- List all IDs

	LIBORdic = {}
	LIBORlis = []

	for spoon in table:
		print(spoon.text)
		LIBORlis.append(spoon.text)

	LIBORdic = {"Index":["Latest", "1wk ago", "High", "Low"], LIBORlis[0]:LIBORlis[1:5], LIBORlis[5]:LIBORlis[6:10], LIBORlis[10]:LIBORlis[11:15], LIBORlis[15]:LIBORlis[16:20], LIBORlis[20]:LIBORlis[21:25]}

	LIBORdf = pd.DataFrame(LIBORdic)
	#LIBORdf = LIBORdf.set_index("Timeline")

	print(LIBORdf.head())

	if(toCSV):
		LIBORdf.to_csv("LIBOR_{}.csv".format(datetime.now().strftime("%m-%d-%y")))

	return LIBORdf


getLIBOR(URL, toCSV = False, page = None)


