#parsePaperShuffle.py

import minecart, os, pandas as pd, numpy
from PyPDF2 import PdfWriter, PdfReader


appsDir = "/Users/dclark/Documents/Reverse Apps/"
scanDir = "/Users/dclark/Documents/USPS Receipt.pdf"

#parsePaperShuffle.py

def scanTextFromPDFv1(send, search):

	directory = appsDir

	for filename in os.listdir(appsDir):

		print(filename)
		p = 0

		#collections of specific pages:
		signed = {}
		samples = []
		signedCopy = []
		borrowerCopy = []

		pdf = open("{}{}".format(appsDir, filename),'rb')

		doc = minecart.Document(pdf)
		for page in doc.iter_pages():
			p += 1
			signatures = []

			# if p > 100:
			# 	print("Page limit reached...")
			# 	break

			for letter in page.letterings:
				#print("///", letter)

				if search[0] in letter:
					signatures.append(str(letter))
					print(letter, "page:", p)

				if search[1] in letter:
					print("End of signed pages")
					#break

			if len(signatures) > 0:
				signed[p] = signatures


			print(filename, "page:", p, "Last Text:", letter)
		
		signedDF = pd.DataFrame.from_dict(signed, orient='index').transpose().transpose().to_csv("{}_Pages.csv".format(filename))
		return signedDF

		break

def scanTextFromPDFv2(send, splice):

	search = ["<BOR_1_SGF>", "SeparatorBorrowerCopy", "SAMPLE"]
	search_CA = ["C1Signature", "Borrower's Copy", "Note to Broker:"]

	for filename in os.listdir(appsDir):

		if ".pdf" in filename:

			print("\nOpening new file:", filename)
			p = 0

			#collections of specific pages:
			signed = set()
			endOfSigned = 0
			excess = []

			reader = PdfReader("{}{}".format(appsDir, filename), strict = False)

			signed = []

			for page in range(len(reader.pages)):

				text = reader.pages[p].extract_text()
				#print("///", text)

				if search[0] in text and endOfSigned == 0:
					signed.append(p)
					print("Sig detected on page:", p+1)

				if search[1] in text:
					endOfSigned = p
					print("_______________End of signed pages_______________")

				if endOfSigned > 0:
					if search[2] or search[3] in letter:
						excess.append(p)
						print("Excess page found")

				p += 1

			print(filename, "| Signatures:", len(signed), "| End of signed copy:", endOfSigned)


			if(splice):

				borrowerCopy = numpy.subtract((range(endOfSigned, len(reader.pages))), excess)
				splitPDF(filename, reader, signed, endOfSigned, borrowerCopy)
			
			#signedDF = pd.DataFrame.from_dict(signed, orient='index').transpose().transpose().to_csv("{}_Pages.csv".format(filename))

			#return signedDF



def splitPDF(filename, reader, signed, endOfSigned, excess):

	try:

		pdfWriterSignatures = PdfWriter()
		pdfWriterSignedCopy = PdfWriter()
		pdfWriterBorrowerCopy = PdfWriter()

		try:
			os.mkdir("{}{}/".format(appsDir, filename.removesuffix(".pdf")))

		except:
			print("Directory already created... Carrying on.")
			
		for page in signed:
			pdfWriterSignatures.addPage(reader.getPage(page))

		for page in range(endOfSigned):
			pdfWriterSignedCopy.addPage(reader.getPage(page))

		for page in range(endOfSigned, len(reader.pages)):
			pdfWriterBorrowerCopy.addPage(reader.getPage(page))

		with open("{}{}/{}_signatures_only.pdf".format(appsDir, filename.removesuffix(".pdf"), filename.removesuffix(".pdf")), 'wb') as file1:
			pdfWriterSignatures.write(file1)
			print("Printed signatures_only copy")

		with open("{}{}/{}_signed_copy.pdf".format(appsDir,filename.removesuffix(".pdf"), filename.removesuffix(".pdf")), 'wb') as file2:
			pdfWriterSignedCopy.write(file2)
			print("Printed signed copy")

		with open("{}{}/{}_borrower_copy.pdf".format(appsDir, filename.removesuffix(".pdf"), filename.removesuffix(".pdf")), 'wb') as file3:
			pdfWriterBorrowerCopy.write(file3)
			print("Printed borrower copy")


	except OSError as err:
		print("OS error: {0}".format(err))

#splitPDF("/Users/dclark/Documents/Reverse Apps/Kirk Application.pdf", [2,5,8,11,13,15], 30)


signedDF = scanTextFromPDFv2(send = False, splice = True)
