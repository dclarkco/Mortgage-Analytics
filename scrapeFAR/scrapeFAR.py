# scrapeFAR.py
# Dillon Clark, Godirect Financial 2022
# Yoinks FAR ratesheet data into a csv to be used for rates. Meant to be automatically run each tuesday morning.

#TODO:
#Send email of rates as well using emailFiles


import numpy, altair, requests, sys, tabula as tb, pandas as pd
from datetime import datetime

sys.path.insert(0, "/Users/dclark/Documents/GitHub/scripts-private/emailFiles")

#import emailFiles

headers = {
	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
	}

now = str(datetime.now().strftime("%m-%d-%y"))

def getPDF(URL):

	pdf = requests.get(URL, headers = headers)

	pdfTitle = "FAR ratesheet {}.pdf".format(datetime.now().strftime("%m-%d-%y"))

	with open(pdfTitle, 'wb') as f:
		f.write(pdf.content)
	print("wrote csv to:", pdfTitle)

	return pdfTitle

def getSoup():


	URL = ("https://totallyreverse.com/assets/FAR/ratesheet.pdf")
	page = requests.get(URL, headers = headers)

	soup = BeautifulSoup(page.content, "html.parser")


	print(soup.prettify())

def minePDF(csv, send):

	print("Initiating tablua read...")
	#

	pdfTitle = getPDF("https://totallyreverse.com/assets/FAR/ratesheet.pdf")
	df1 = tb.read_pdf(pdfTitle, pages = 2, area = (176,57,303,560), columns = (102, 138, 177, 216, 255, 294, 331, 369, 406, 444, 481, 520), stream = True, multiple_tables=False)
	df2 = tb.read_pdf(pdfTitle, pages = 2, area = (345,57,473,560), stream = True, multiple_tables=False)
	
	df1 = pd.DataFrame(df1[0]).iloc[1:, :]
	df2 = pd.DataFrame(df2[0]).iloc[1:, :]

	df1.columns = ["Margin (1yr CMT)", "Initial Rate (1yr CMT)", "Expect. Rate (1yr CMT)", " Premium (By PLU%) =>> 0.01%-10%", "10.01%-20%", "20.01%-30%", "30.01%-40%", "40.01%-50%", "50.01%-60%", "60.01%-70%", "70.01%-80%", "80.01%-90%", "90.01%-100%"]
	df2.columns = ["Margin (1yr CMT)", "Initial Rate (1yr CMT)", "Expect. Rate (1yr CMT)", " Premium (By PLU%) =>> 0.01%-10%", "10.01%-20%", "20.01%-30%", "30.01%-40%", "40.01%-50%", "50.01%-60%", "60.01%-70%", "70.01%-80%", "80.01%-90%", "90.01%-100%"]

	df1.style.set_caption("FAR Monthly Adjustable Rate HECM (10% Life Rate Cap) - 10 Day Lock")
	df2.style.set_caption("FAR Monthly Adjustable Rate HECM (5% Life Rate Cap) - 10 Day Lock")

	#df1 = df1.set_index("Margin (1yr CMT)")
	#df2 = df2.set_index("Margin (1yr CMT)")


	if(csv):
		df1.to_csv('FARratesheet_10%_{}.csv'.format(now))
		df2.to_csv('FARratesheet_5%_{}.csv'.format(now))
		print("wrote dataframes to csv files")

	if (send):
		import emailFiles
		attach = [('FARratesheet_5%_{}.csv'.format(now)), ('FARratesheet_10%_{}.csv'.format(now)), pdfTitle]
		emailFiles.send_mail(toaddr = "dillon@poweredbyqed.com", subject = "FAR rate sheet: {}".format(now), message = ["FAR Monthly Adjustable Rate HECM (10% Cap) - 10 Day Lock \n", df1, "\n FAR Monthly Adjustable Rate HECM (5% Cap) - 10 Day Lock\n", df2], attach = attach)

	return df1, df2
	#tabula.convert_into("test.pdf", "output.csv", output_format="csv", pages='all')



df1, df2 = minePDF(csv = True, send = False)

print(df1.head(10))
print(df2.head(10))
