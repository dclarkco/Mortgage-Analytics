import pandas as pd
from datetime import datetime
import sys

def checkAddress(filename):

	from usps import Address, USPSApi

	usps = USPSApi('544QED006465', test=True)

	df = pd.read_excel(filename)

	#print(df.columns, df.head())

	for row in df.itertuples():
		orig_addr = Address(
			name = str(row[7]),
			address_1 = str(row[9]),
			city = str(row[10]),
			state = str(row[11]),
			zipcode = str(row[12])
			)
		try:
			validation = usps.validate_address(orig_addr)
		except:
			time.sleep(30)
			try:
				validation = usps.validate_address(orig_addr)
			except:
				print("Connection timeout error...")
				time.sleep(60)

		validAddress = list(validation.result['AddressValidateResponse']['Address'].values())

		corrected_addr = Address(
			name = str(row[7]),
			address_1 = str(validAddress[2]),
			city = str(validAddress[3]),
			state = str(validAddress[4]),
			zipcode = str(validAddress[5])
			)

		compareAddresses(orig_addr, corrected_addr)

def compareAddresses(original, corrected):

	mismatch = False

	if original.name.upper() != corrected.name.upper():
		print("{} - corrected to: {}".format(original.name, corrected.name))
		mismatch = True

	if original.address_1.upper() != corrected.address_1.upper():
		print("{} - corrected to: {}".format(original.address_1, corrected.address_1))
		mismatch = True

	if original.city.upper() != corrected.city.upper():
		print("{} - corrected to: {}".format(original.city, corrected.city))
		mismatch = True

	if original.state.upper() != corrected.state.upper():
		print("{} - corrected to: {}".format(original.state, corrected.state))
		mismatch = True

	if original.zipcode.upper() != corrected.zipcode.upper():
		print("{} - corrected to: {}".format(original.zipcode, corrected.zipcode))
		mismatch = True

	if mismatch:
		print("\nFrom : {} {} {} {} {}".format(original.name,original.address_1,original.city,original.state,original.zipcode))
		print("To   : {} {} {} {} {} \n".format(corrected.name,corrected.address_1,corrected.city,corrected.state,corrected.zipcode))
	else:
		print("No mismatch found for {}".format(original.address_1))

def createURL(filename):

	fileLocation = "data/{}".format(filename)

	df = pd.read_excel(fileLocation)

	df = df[['flyer', 'valuation', 'full_street_address', 'address_house_number',
	 'address_direction_left', 'address_street_name', 'address_mode', 'address_city', 'address_state', 'address_zip',
	 'latitude', 'longitude', 'year_built', 'val_esc_factor', 'val_base_date', 'val_index_at_base', 'val_index_current']]


	df = df.sample(10)
	df = df.reset_index()

	df['valuation'] = df['valuation'].round(0)
	print(df.shape, df.head(5))

	rows = list(range(len(df.index)))
	#cols = list(range(len(df.columns)))

	# 3. Perform processing

	for i in rows: # iterate row-wise (using int index)

		valuation = df.at[i,'valuation']

		address =  str(df.at[i,'full_street_address']).lower().replace(' ', '-')
		city = str(df.at[i,'address_city']).lower().replace(' ', '-')
		state = str(df.at[i,'address_state'])
		zipCode = str(df.at[i,'address_zip'])
		url = 'https://www.zillow.com/homes/{}-{},-{}-{}_rb/'.format(address, city, state, zipCode)
		zaluation = checkValue(url)
		diff = zaluation - valuation
		
		df.at[i, 'URL'] = url
		df.at[i, 'Zaluation'] = zaluation
		df.at[i, 'val_difference'] = diff


		print(i,'/', rows[-1], '|', url, '|', address, '|', valuation, '|', zaluation, '|', diff)



	df = df.drop(0)
	df = df.set_index('index')
	df.to_csv('{}_with_Zurl.csv'.format(filename))
	print('wrote csv out')

# adapting code from https://github.com/maxwellbade/zillow_scrape_python/blob/master/zillow_final.ipynb

def checkValue(url):
	
	import requests, time, html5lib, re, random
	from bs4 import BeautifulSoup as bs

	print("Scraping: ", url)
	headers = {
		'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:105.0) Gecko/20100101 Firefox/105.0',
		'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
		'Accept-Language': 'en-US,en;q=0.5',
		'Accept-Encoding': 'gzip, deflate, br'
	}

	r = requests.get(url = url, headers = headers)

	soup = bs(r.text, 'html5lib')

	#print(soup.prettify())

	try:
		Zaluation = soup.find(class_ = 'Text-c11n-8-73-0__sc-aiai24-0 xGfxD')
		Zaluation = re.sub('[$,]', '', str(Zaluation.get_text()))
		Zaluation = int(Zaluation)

	except:
		print("Valuation failed!")
		time.sleep(120)
		try:
			Zaluation = soup.find(class_ = 'Text-c11n-8-73-0__sc-aiai24-0 xGfxD')
			Zaluation = re.sub('[$,]', '', str(Zaluation.get_text()))
			Zaluation = int(Zaluation)
		except:
			Zaluation = 0
			print("Zaluation cannot complete!")

	print("sleeping...")
	time.sleep(random.randint(3,10))

	return(Zaluation)

def createTest():

	df = pd.read_excel('data/MC7701_CO_FHA_to_reverse.xlsx')

	print(df.head())


#createTest()
createURL('MC_Test.xlsx')
# createURL('MC7701_CO_FHA_to_reverse.xlsx')
# createURL('MC7702_CO_VA_to_reverse.xlsx')
# createURL('MC7703_CO_Conv_to_reverse_2010-2012.xlsx')
# createURL('MC7704_CO_Conv_to_reverse_2013-2016.xlsx')
# createURL('MC7705_CA_FHA_to_reverse.xlsx')

# for key, item in validation.result['AddressValidateResponse'].items():
# print(key, item)


# addr = Address(
# 	name = 'Jim Doe',
# 	address_1 = '25 Belvedere',
# 	city = 'Aliso Viejo',
# 	state = 'CA',
# 	zipcode = '92656'
# 	)


#validation = usps.validate_address(addr)
#print(validation.result)

# for key, item in validation.result['AddressValidateResponse'].items():
# 	print(key, item)