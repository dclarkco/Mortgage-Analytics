# housingIndex.py | Dillon Clark 2022
# QED Analytics proprietary data solutions
# Cleans, extrapolates, and exports as Json ZHVI data.


import numpy as np, pandas as pd

#import os

#os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray 
#import modin.pandas as pd - Borked for the meantime (Allows for multithreading, but doesnt work well and actually slows the code down)


def proccessZHVI(toJson = False, toCsv = True, sample = 0, debug = False):

	# Read in zhvi data from raw spreadsheet

	df = pd.read_csv("Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv")

	# Drop unused columns

	df.drop(["RegionID", "StateName", "RegionType", "SizeRank", "State", "City", "Metro", "CountyName"], axis = 1, inplace = True)

	# If testing features or a unique subset must be created, call function with desired sample size (sample = #)
	# Else, sampling will be disabled and a full set will be exported.

	if sample > 0:
		df = df.sample(sample)

	# Clerical work, pre-processing. (Rename RegionName to zipCode, and convert to strings for processing later)

	df = df.rename(columns = {'RegionName':'zipCode'})
	df['zipCode'] = df['zipCode'].astype('str')

	# Convert short (4 digit) zipCode values to 5 digits. Some redundancy to verify output is string.

	for idx, row in df.iterrows():

		if len(str(row['zipCode'])) < 5:
			print("zipCode prepended with leading zero: {}".format(df.at[idx, 'zipCode']))
			df.at[idx, 'zipCode'] = '0' + str(row['zipCode'])

		else:
			df.at[idx, 'zipCode'] = str(row['zipCode'])


	# Processing:


	# Set zipCode as index, sort by index, set index type as string

	df = df.set_index('zipCode')
	df = df.sort_index(axis = 0)
	df.index = df.index.astype('str')


	# Interpolate missing data:

	df = df.interpolate(axis = 'columns', method = 'linear')

	# Round all values to nearest integer, convert value columns to Int64 (preserves nulls, if any. To be counted later with debug ON)
	
	df = df.round(0)
	df[0:] = df[0:].astype('Int64')

	# Generate default value series (average of all values per each column, for use when zipCode code mismatches happen)

	defaultSeries = pd.DataFrame(df.mean(axis = 0).round(0), columns = ["Default"])
	defaultSeries = defaultSeries.transpose() # needed when converting a series to a DF (indices are transposed)

	print(defaultSeries.index, df.index) # check index is aligned
	print(defaultSeries.head())

	# Stick em together.

	df = pd.concat([defaultSeries, df], axis = "index")

	# Begin extrapolation
	# 1. Reverse orientation of columns index (now sorted by most recent)

	df = df[df.columns[::-1]]

	print(df.columns[0:10])

	# 2. Generate integer index of the dataframe to iterate over

	rows = list(range(len(df.index)))
	cols = list(range(len(df.columns)))  #list(reversed(range(len(df.columns))))

	# 3. Perform processing

	for i in rows: # iterate row-wise (using int index)
		for c in cols: # iterate column-wise (using int index on reversed dataset)
			if pd.isna(df.iat[i,c]): # If value is NaN

				print("Index:{} Column:{} | Indices: {} : {}".format(i, c, df.index[i], df.columns[c]))

				#print(df.iloc[i,c-5:c], "|\n")
				#print(df.iloc[i,c:c+5], "------> c+1", df.iloc[i,c-1])
				#print(df.iat[i,c-1])

				y = df.iat[i,(c-1)] #   y = previous (following) month valuation
				x = 0   #df.iat[i, c] | x = value of current month (current cell which is NaN)

				A = df.iat[0,c] #       A = Default index of current month
				B = df.iat[0,(c-1)] #   B = Default index of previous (following) month

				x = ((y*A)/B)
				#print(x)

				print("{} = {}*{}/{}".format(x, y, A, B)) # print formula to console for verifcation purposes

				df.iat[i, c] = x  # set the cell to x

	# 4. Undo reverse orientation of columns index (now sorted by least recent)

	df = df[df.columns[::-1]]

	# 5. Round any remaining float values down to int (important this is done post-calculus; preseves accuracy of calculations)

	df = df.round(0)

	# Post processing (duplicate and move data)

	# Insert "Current column" as duplicate of latest date column.

	df.insert(loc = 0, column = "Current", value = df.iloc[:,-1])
	print(df.head())

	# Add index title

	df.index.name = 'zipCode'

	# END PROCESSING!!

	if (debug):
		print("\n----------------BEGIN DEBUG----------------\n")
		print("\nIndex value type: {}".format(type(df.index.values[1])))
		print("\nFirst column: {}".format(df.columns[0]))
		print("\nLast column: {}".format(df.columns[-1]))
		print("\nNumber of columns (months): {}".format(len(df.columns)))
		print("\nIndices (zipCodes): {}".format(df.index))
		print("\nNumber of null cells: {}".format(df.isna().sum().sum()))
		print("\n----------------END DEBUG----------------\n")

	# OUTPUT

	if (sample > 0):
		nameCSV = "{}_sampled_zhvi.csv".format(sample)
		nameJson = "{}_sampled_zhvi.json".format(sample)

	elif (sample == 0):
		nameCSV = "complete_zhvi.csv"
		nameJson = "complete_zhvi.json"

	if (toCsv):
		df.to_csv(nameCSV)

	if (toJson):

		import json

		out = open(nameJson, 'w')

		jsonList = []

		rows = list(range(len(df.index)))

		jsonDF = df.reset_index()

		for i in rows:   # iterate through rows, writing a new json/dict for each one, to be appended to the master list for output later
			row = jsonDF.iloc[i]
			squash = row.squeeze().convert_dtypes(convert_integer = True) 	# squeeze turns a single row into a series object, which is easier to work with
			squash = row.squeeze().to_dict()							  	# series -> dict

			squash_key = squash['zipCode']								  	# grab the zip code key/value pair
			squash.pop('zipCode')											# eject the zip code w/ pop, get only the values

			print(squash_key, squash)

			for key, value in squash.items():
				squash[key] = int(value)

			tempDict = {"zipCode":squash_key, "indices":squash}				# temporary dict to be added to master list. This represents a single json page

			if debug:

				print(tempDict.items())

			json.dump(tempDict, out, indent = 4)
			# jsonList.append(tempDict)										# append individual jsons to master list.
			#print(jsonList[i])


		#print(jsonList[0:3])
		# for item in jsonList:												# iterate through 
		# 	json.dump(item, out, indent = 4, )

		out.close()
	

def transposeDF(file): # transpose if needed.

	df = pd.read_csv(file, low_memory = False)
	df = df.set_index('zipCode')

	print("Number of null cells: {}".format(df.isna().sum().sum()))

	df = df.transpose()

	df.index.name = 'Date'

	df.to_csv("transposed_{}".format(file))



proccessZHVI(toCsv = True, toJson = True, sample = 0, debug = False)
#zips = range(80010, 80247)


#transposeDF("100_sampled_zhvi.csv")

