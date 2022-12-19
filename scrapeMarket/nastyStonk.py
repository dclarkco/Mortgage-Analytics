import nasdaqdatalink, re, sys, os, requests
import pandas as pd
import waybackmachine as wm
import yfinance as yf
import numpy as np
from datetime import datetime, date
from bs4 import BeautifulSoup

# from tensorflow import keras
# from tensorflow.keras import layers

apikey = 'qmT62ueF51hLvYkbcEQH'

debug = True

startDate = '2000-01-01'
endDate = datetime.now().strftime("%Y-%m-%d")
print(endDate)

#Queries:

#stonk - searches for relevant stonks and other economic indicators. Geared toward ML usage. (high record, high features)
search_stonk = ['FRED/GDP', 'FRED/GDPC1', 'FRED/CPIAUCSL', 'FRED/CPILFESL', 'FRED/BASE', 'FRED/M1', 'FRED/DFF', 'FRED/DGS5',
'FRED/DGS10', 'FRED/DGS30', 'FRED/T5YIE', 'FRED/DPRIME', 'FRED/UNRATE', 'FRED/NROU', 'FRED/NROUST', 'FRED/CIVPART', 'FRED/MEHOINUSA672N',
'FRED/DSPI', 'FRED/DSPIC96', 'FRED/INDPRO', 'FRED/TCU', 'FRED/HOUST', 'FRED/CP', 'FRED/STLFSI', 'FRED/GFDEBTN', 'CUR/EUR', 'CUR/CNY', 'CUR/GBP',
'ECONOMIST/BIGMAC_USA', 'ECONOMIST/BIGMAC_CHN', 'ECONOMIST/BIGMAC_EUR', 'WIKI/AAPL', 'WIKI/MSFT', 'WIKI/GOOG', 'WIKI/AMZN', 'WIKI/TSLA',
'WIKI/UNH', 'WIKI/V', 'WIKI/NVDA', 'WIKI/XOM', 'WIKI/WMT', 'WIKI/MA', 'WIKI/JPM', 'WIKI/CVX', 'WIKI/BAC', 'WIKI/DHR', 'WIKI/ORCL', 'WIKI/CSCO',
'WIKI/NKE', 'WIKI/INTC']

#consumer
search_consumer = ['ECONOMIST/BIGMAC_USA', 'ECONOMIST/BIGMAC_CHN', 'ECONOMIST/BIGMAC_EUR', 'WIKI/AAPL', 'WIKI/MSFT', 'WIKI/GOOG', 'WIKI/AMZN', 'WIKI/TSLA',
'WIKI/UNH', 'WIKI/V', 'WIKI/NVDA', 'WIKI/XOM', 'WIKI/WMT', 'WIKI/MA', 'WIKI/JPM', 'WIKI/CVX', 'WIKI/BAC', 'WIKI/DHR', 'WIKI/ORCL', 'WIKI/CSCO',
'WIKI/NKE', 'WIKI/INTC']

search_index = {'FRED/USDONTD156N': 'LIBOR overnight USD', 'FRED/USD1WKD156N': 'LIBOR 1wk USD', 'USTREASURY/YIELD': 'CMT', 'USTREASURY/REALYIELD':'Real-CMT'}
index_headers = ['LIBOR overnight USD', 'LIBOR 1wk USD', 'CMT', 'Real-CMT']


def getNasty(ticker, out, startDate, endDate):
	data = (nasdaqdatalink.get(ticker, start_date = startDate, end_date = endDate, api_key = apikey))
	try:
		print('\n',ticker)
		print(data.head())
	except:
		print("Dataset does not contain data...")
	if out:
		filename = 'data_{}.csv'.format(ticker.replace('/', '_'))
		print(filename)
		data.to_csv(filename)
	return data

#begin spaghet
def collectNasty():

	fullDF = pd.DataFrame(columns = ["Date"], index = (pd.date_range(start = startDate, end = endDate)))

	for query, text in search_index.items(): 			# iterate through all search strings (one of teh query lists)
		data = (getNasty(query, out=False, startDate = startDate, endDate = endDate)) # send query to get function, returns data in DF format from NDQ
		data.to_csv(("{}.csv".format(query)).replace('/', '_')) # export raw data to csv for recordkeeping

		if len(data.columns) > 1: 						# if data contains more than one column (See: BIGMACINDEX or tickers (open/close/avg))
			fix = str(re.sub(r'^.*?/','', text)+" | ") 	# use regedit to remove everthing before the / in search query and format a new string for the file header
			data = data.add_prefix(fix)					# add prefix to all column headers
			print(fix, len(data.columns), data.columns)

			for column in data.columns:					# iterate through all columns within data subset
				fullDF = fullDF.join(data[column])		# add data to master DataFrame
				print("Joined", column)					# printout column when appended

		else:											# if data is a singular column / series
			print("SINGLE COLUMN QUERY ________________________________")
			
			fullDF[text] = data						# stick it, slap it, send it.

	LIBOR_1mo = pd.read_csv('/Users/dclark/Documents/GitHub/python-scripts/scrapeMarket/LIBOR_1mo.csv', 
		parse_dates = True, infer_datetime_format = True, keep_date_col = False, index_col = 'Date').rename(columns = {'Open' : 'LIBOR_1mo'})
	LIBOR_1y = pd.read_csv('/Users/dclark/Documents/GitHub/python-scripts/scrapeMarket/LIBOR_1y.csv', 
		parse_dates = True, infer_datetime_format = True, keep_date_col = False, index_col = 'Date').rename(columns = {'Open' : 'LIBOR_1y'})

	LIBOR_1mo = LIBOR_1mo['LIBOR_1mo'].str.strip('%').astype(float)
	LIBOR_1y = LIBOR_1y['LIBOR_1y'].str.strip('%').astype(float)

	fullDF = fullDF.join(LIBOR_1mo)
	fullDF = fullDF.join(LIBOR_1y)

	fullDF = reorder_columns(dataframe = fullDF, col_name = 'LIBOR_1mo', position = 0)
	fullDF = reorder_columns(dataframe = fullDF, col_name = 'LIBOR_1y', position = 0)

	print("{} Datasets downloaded...".format(len(search_index)))
	print("{} Datasets concatenated...".format(len(fullDF.columns)))

	fullDF = fullDF.drop(columns = 'Date')				# drop extra date column (no idea why this gets made, or why its empty now)
	fullDF = fullDF.sort_index(ascending = False)		# sort by date (descending)
	#fullDF = fullDF.dropna()									# drop na rows (BOOOORK)

	#interpolate
	for item in fullDF.index:
		print(item)
	
	fullDF.interpolate(method="linear", limit_direction = "both", limit_area = "inside") # 

	#fullDF = pd.concat([fullDF, pd.DataFrame(fullDF.columns)])
	fullDF = fullDF.fillna(method = 'ffill')
	fullDF = fullDF.fillna(method = 'bfill')

	if debug: 											# descriptive statistics
		print(fullDF.head())
		print(fullDF.isnull().sum())

	fullDF.to_csv('CEI.csv') # Combined Economic Indicators
	#fullDF.to_csv('S&P.csv')

	return fullDF


# scrapeMarket.py
# Dillon Clark, Godirect Financial 2022
# retreives LIBOR data from WSJ each week



def reorder_columns(dataframe, col_name, position):
    """Reorder a dataframe's column.
    Args:
        dataframe (pd.DataFrame): dataframe to use
        col_name (string): column name to move
        position (0-indexed position): where to relocate column to
    Returns:
        pd.DataFrame: re-assigned dataframe
    """
    temp_col = dataframe[col_name]
    dataframe = dataframe.drop(columns=[col_name])
    dataframe.insert(loc=position, column=col_name, value=temp_col)

    return dataframe

def getIngredients(URL):

	headers = {
		'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
		}

	page = requests.get(URL, headers = headers)
	return page

def getLIBOR(URL, toCSV):

	#page = getIngredients(URL)

	page = wm.fetch(URL, date = datetime.strptime(endDate, '%Y-%m-%d')).response
	print(type(page))

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
		LIBORdf.to_csv("LIBOR_{}.csv".format(datetime.now().strftime("%Y-%m-%d")))

	return LIBORdf

def batchProcess(out, compare_headers, compare_dates):

	fullDF = pd.DataFrame() #establish empty dataframe to fill with concatenated dataframes (from dataFrames list)
	dataFrames = [] #establish empty list of data frames to be concatenated later

	DBdir = "/Users/dclark/Documents/GitHub/python-scripts/scrapeMarket/LIBOR/BatchProcessing"

	for filename in os.listdir(DBdir):  
		for root, dirs, files in os.walk(os.path.join(DBdir, filename)): # iterate through folders in BatchProcessing ->os.walk
			for file in files: 
				#print(root,dir,file)

				if "~" in file or ".DS_Store" in file: # skip null macOS files (excel prepends with ~ for temp or autosave files, which break this code)
					print("Null file skipped...")
					None

				#if excel and ".xlsx" in file:

				elif ".csv" in file: # code to parse CSV files instead of XLSX
					path = os.path.join(root, file)
					print(path)

					tempDF = pd.read_csv(path, low_memory = False, parse_dates = ["Date"], infer_datetime_format=True).sort_values("Date")
					#tempDF['Date'] = pd.to_datetime(tempDF["Date"], format = "%m-%d-%Y", utc=True)

					print(file, "| Length:", len(tempDF), "| Start date:", tempDF["Date"].iloc[0], "| End date:", tempDF["Date"].iloc[-1])
					
					tempDF.drop(['Close', 'High', 'Low'], axis =1, inplace=True)

					dataFrames.append(tempDF)

				else:
					#print("Null file:", file)
					None
				
				#break
		# break

	fullDF = pd.concat(dataFrames, verify_integrity = False).sort_values("Date").reindex() # concatenate dataframes into one, sort values by date, reset index
	print(fullDF.columns)

	fullDF.set_index("Date", inplace = True)
	
	#print(type(fullDF.index[0])) # check type

	if compare_dates:
		compareDates(fullDF)

	if out:
		fullDF.to_csv(os.path.join(DBdir, "Complete.csv"))
				
	#print(fullDF["Date"].describe(datetime_is_numeric=True))
	#print("\nRates:\n", fullDF.describe())

	return fullDF

def compareDates(fullDF):

	# TODO:
	# exclude holidays from comparison matrix

	allDates = pd.date_range(start = '2016-09-01', end = "2012-09-06", freq='B') # create index of 'business' dates between two dates (skips weekends)
	mortgageDates = pd.to_datetime(fullDF["Date"]).unique() # export all unique dates from dataset
	missingDates = [] 

	print("DATE TIME DATA SIZE:", "CAPTURED DATES:", len(mortgageDates),"| ALL DATES: ", len(allDates))
	print("DATE TIME DATA TYPE:", "CAPTURED DATES:", type(mortgageDates),"| ALL DATES: ", type(allDates))
	print("DATE TIME DATA UNITS:", "CAPTURED DATES:", type(mortgageDates[1]),"| ALL DATES: ", type(allDates[1]))


	for date in allDates:
		if date in mortgageDates: # output all found or not found dates by comparing iteratively
			print(date, "Found...") # probablly really inefficient, but short runtime isn't the goal here
		else:
			missingDates.append(date)
			print(date, "NOT Found...")

	print("\nMissing Days:", len(missingDates), "/", allDates) # output total amount of missing days. usually at least 20-100, as holidays are still counted.

	return missingDates

def combineAll():

	ceiDF = pd.read_csv('/Users/dclark/Documents/GitHub/python-scripts/scrapeMarket/CEI.csv', index_col='Unnamed: 0')
	LIBOR_1mo = pd.read_csv('/Users/dclark/Documents/GitHub/python-scripts/scrapeMarket/LIBOR_1mo.csv', index_col='Date')
	LIBOR_1y = pd.read_csv('/Users/dclark/Documents/GitHub/python-scripts/scrapeMarket/LIBOR_1y.csv', index_col='Date')

	print(ceiDF.head() ,'\n', LIBOR_1y.head(),'\n', LIBOR_1mo.head())
	print(ceiDF['LIBOR overnight USD'].describe())

	for row in ceiDF.itertuples():
		print(type(row[0]))
		try:
			print(LIBOR_1mo.at[row[0],'Open'])
		except:
			print("Date not found in addendum set")
		break

	for row in LIBOR_1mo.itertuples():
		print(type(row[0]),row[0])


	print(ceiDF.describe(), ceiDF.tail())

def yahooStonk():
	SP500 = yf.Ticker("^GSPC")
	SP500_df = (SP500.history(period="max"))
	print(SP500_df.tail())
	SP500_df.to_csv("SP500.csv")


#yahooStonk() 

#batchProcess(out = True, compare_headers = False, compare_dates = False)

#getLIBOR("https://www.wsj.com/market-data/bonds", toCSV = True)

#combineAll() borked I think?

collectNasty()



