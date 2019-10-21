#!/usr/bin/env python3

"""
Written by: Emma Bailin
Created on: October 15, 2019
Last Modified: NA
Description: Identifies the duplicate Digital Certificates based on their leaf certificate fingerprint, and generate an output file with sets of duplicate input rows, one row per set of duplicates. 
"""
"""
Pre-coding Notes:
	1) Output is a dictionary, so presumably, converting the JSON object into a dictionary

	2) Brute force ==> read in each json, compare to contents in dictionary, if not there, put in dict, otherwise, put json dict into hash of repeats for eventual return. Rinse. Repeat

	3) Known problem: We can't just compare dictionaries if there are lists inside the dictionary, the lists may be out of order and it'll miss repeats. Solution: Make sure the orders are the same. 
		Could sort each column in dict before putting in bigger?
		--> Let's start there and see if we can improve later. 
		**-->WAIT!!! NO! We only need to look at the fingerprints! The fingerprints aren't lists! So we don't actually care about the sorting!

	4) PANDAS dataframe is better suited to huge datasets than dicts. It may be better to store the info in there. 

So The PLAN:
	json=loads(file)
	data=json[] 

	#sort
	for col in data:
		look for any lists-->yes--> order --> repeat
		--> no --> continue
	
	#dump into data.frame [OR is it faster to append to list and then put list of dicts into dataframe? Yeah, that's faster, according to stackoverflow]
 	datas.append(data)

	files=pd.DataFrame(datas)

	#now we can do comparisons
	( What do you know! PANDAS has a function for that! it's just pd.DataFrame.duplicated. ) [Perhaps look at other methods and timeit?]
	dups=files.duplicated()

	dups.write_csv()
"""
import json
from io import StringIO
from os import path, getcwd, mkdir
import ijson.backends.yajl2 as ijson # WAY faster for importing and working with jsons (https://pypi.org/project/ijson/)
import sys
import pandas as pd
from pandas.io.json import json_normalize
from functools import reduce
from glob import glob
import timeit #testing purposes
import timing #for complete script timings. Taken from an answer on stackoverflow years ago. NOT MINE
import dask.dataframe as dd
# from multiprocessing import cpu_count, Queues #so I can tell how many partitions 
# import multiprocessing as mp
# import pandas.util.testing as pdt #to confirm the multiprocessing worked

metas={'cert_index': 'int64', 'cert_link': 'object', 'chain': 'object', 'seen': 'float64', 'update_type': 'object', 'leaf_cert.all_domains': 'object', 'leaf_cert.as_der': 'object', 'leaf_cert.extensions.authorityInfoAccess': 'object', 'leaf_cert.extensions.authorityKeyIdentifier': 'object', 'leaf_cert.extensions.basicConstraints':'object', 'leaf_cert.extensions.certificatePolicies': 'object', 'leaf_cert.extensions.ctlSignedCertificateTimestamp': 'object', 'leaf_cert.extensions.extendedKeyUsage': 'object', 'leaf_cert.extensions.keyUsage': 'object', 'leaf_cert.extensions.subjectAltName': 'object', 'leaf_cert.extensions.subjectKeyIdentifier': 'object', 'leaf_cert.fingerprint': 'object', 'leaf_cert.not_after': 'int64', 'leaf_cert.not_before': 'int64', 'leaf_cert.serial_number': 'object', 'leaf_cert.subject.C': 'object', 'leaf_cert.subject.CN': 'object', 'leaf_cert.subject.L': 'object', 'leaf_cert.subject.O': 'object', 'leaf_cert.subject.OU': 'object', 'leaf_cert.subject.ST': 'object', 'leaf_cert.subject.aggregated': 'object', 'source.name': 'object', 'source.url': 'object', 'leaf_cert.extensions.ctlPoisonByte': 'bool', 'leaf_cert.extensions.crlDistributionPoints': 'object', 'leaf_cert.extensions.extra': 'object', 'leaf_cert.extensions.issuerAltName': 'object'}

'''
HELPERS
'''
'''
#Note: Found this on stackOverflow [https://stackoverflow.com/questions/25851183/how-to-compare-two-json-objects-with-the-same-elements-in-a-different-order-equa], accepted answer. Didn't want to reinvent the wheel... Renamed the function to fit with my naming scheme

Recursive method to go through a dict and look for any lists. If a list is found, it is sorted. 
Params:	an object
Returns: the object, but with any lists sorted
'''
def sortDictLists(data):
	if isinstance(data, dict):
		return sorted((k, sortDictLists(v)) for k, v in data.items())
	if isinstance(data, list):
		return sorted(sortDictLists(x) for x in data)
	else:
		return data

#Noted copied from https://stackoverflow.com/questions/24448543/how-would-i-flatten-a-nested-dictionary-in-python-3
def flattenDict(current, key, result):
	# print("type(current): ", type(current))
	if isinstance(current, dict):
		for k in current:
			new_key = "{0}.{1}".format(key, k) if len(key) > 0 else k
			flattenDict(current[k], new_key, result)
	# if isinstance(current, list):
	# 	flattenDict(sorted(x for x in current), key, result)
	else:
		result[key] = current
	# print("result: ", result)
	return result


'''
Takes in a json and returns a sorted/organized dictionary
Params: a json string
Returns: an organized dictionary
'''
def prepRow(jsonStr):
	# print(json.loads(jsonStr))
	# print(type(json.loads(jsonStr)))
	# return sortDictLists(json.loads(jsonStr[0]))
	cert=flattenDict(json.loads(jsonStr), '', {})
	cs={}
	for c in jsonStr:
		# print("c: ", c)
		cs.update({c[0]: c[1]})
	# print("*****type: ", type(cert))
	# print("cert: ",cert)
	# certs.append(cs)
	return cs

	# return flattenDict(ijson.parse(jsonStr), '', {})

def main(argv):
	file="/home/lnvp-linux-wkst1/Desktop/future/subsample3"
	timing.log("Started V2")
	usingJson_v2(file)
	timing.endlog()
	timing.log("Started v1")
	usingJson(file)
	print("done")
	timing.endlog()


	'''
	#get file:
	file=argv
	file="/home/lnvp-linux-wkst1/Desktop/future/ctl_records_subsample"


	certs=[] # for holding all of the ordered data
	#from https://stackoverflow.com/questions/37200302/using-python-ijson-to-read-a-large-json-file-with-multiple-json-objects
	with open(file, encoding="UTF-8") as json_file:
	    cursor = 0
	    for line_number, line in enumerate(json_file):
	        print ("Processing line", line_number + 1,"at cursor index:", cursor)
	        line_as_file = io.StringIO(line)
	        # Use a new parser for each line
	        json_parser = ijson.parse(line_as_file)
	        cert={}
	        # print("json_parser: ", json_parser)
	        for prefix, kind, value in json_parser:
	            # print ("prefix=",prefix, "type=",kind, "value=",value)
	            if "string" is kind:
	            	cert.update({prefix:value})
        	certs.append(cert)
	        cursor += len(line)

	'''
	'''
	with open(file, "r") as f:
		for line in f:
			# print(line)
			# cert=sortDictLists(prepRow(line))#, '', {})
			cert=prepRow(line)#, '', {})

			#we need to put the strings from sortDict back into dicts. 
			#MAYBE THIS ISN'T NECESSARY?!
			cs={}
			for c in cert:
				print("c: ", c)
				cs.update({c[0]: c[1]})
			# print("*****type: ", type(cert))
			# print("cert: ",cert)
			# certs.append(cs)
			certs.append(cert)
	'''
	'''

	certsDF=pd.DataFrame(certs)
	print("dim(certsDF): ", certsDF.shape)
	print("data.columns: ", certsDF.columns)
	certsDF.to_csv("subsample_ijson_pd.csv")

	# print()
	dups=certsDF.duplicated(subset='data.leaf_cert.fingerprint')
	dups=certsDF[dups]
	print(dups)

	'''
"""
'''
With dask proving to be just too much work to get working in such a short time, I'm going to try readInOutIn with multiprocessing

Plan: read in with Pandas and chunks --> put the chunks into a queue (or the like)--> have workers do the normalizing --> create a new dataframe at the end
'''
#THIS IS MODIFIED FROM https://stackoverflow.com/questions/26784164/pandas-multiprocessing-apply
def process_apply(x):
	# new=x['data'].apply(json.dumps)
	# new=json_normalize(new.apply(json.loads))
	if x == "data":
		return json_normalize(json.loads(json.dumps(x)))
	return x

def process(df):
	return df.apply(process_apply, axis=1)

def usingMultiProces(file):
	#read in with pandas
	readers=pd.read_json((file),lines=True, chunksize=100000, dtype=False)
	q=Queue(100) #max size?
	for reader in readers:
		q.put(reader)

"""

'''
As I was thinking about getting readInOutIn to work, it hit me that I've been thinking about this a consecutive manner. There's only so far I can go with it. I need to think about serializing, ergo, THREADING! 
	
I know that reading in a file shouldn't be threaded, but given that that's not the part that takes forever (based on my previous attempts). That part is the processing the chunks (i.e. the normalizing) portion. So I can thread that. 

Just as I was about to add threading to my usingPANDAS framework, I decided to google adding threading to PANDAS. This lead to DASK! Rather than reinventing the wheel, I'm using it. https://docs.dask.org/en/latest/dataframe-api.html

'''
def usingDASK(file):
	chunks=[]
	timing.log("Starting reading-in")

	reader=dd.read_json(file, lines=True, blocksize=2**22, meta={'data': object, 'message_type':object})
	# t=reader['data'].map_partitions(lambda df: df.apply(lambda x: x.apply(flattenDict, key='', result={}))).to_bag()
	# t=reader.map_partitions(lambda df: df['data'].apply(flattenDict, key='', result={})).to_bag()
	datas=reader['data'].map_partitions(lambda df: df.apply((lambda row: flattenDict(row, '', {})))).to_bag()
	new=datas.to_dataframe()
	new['message_type']=reader['message_type']
	new=new.compute()
	dups=new.duplicated(subset='leaf_cert.fingerprint')
	dups=new[dups]
	dups.to_csv("duplicates_DASK.csv")

	'''
	python -m timeit "from searchDuplicateCrts import usingDASK; usingDASK('/Users/esbailin/Desktop/future/bailin/subsample2.dms')"
	
	Timeit for blocksize: 2**28: 1 loop, best of 5: 10.9 sec per loop
	Timeit for blocksize: 2**30: 1 loop, best of 5: 12.1 sec per loop (2**30 is my psutil.virtual_memory().total / psutil.cpu_count())

	python -m timeit "from searchDuplicateCrts import usingDASK; usingDASK('/Users/esbailin/Desktop/future/bailin/subsample3.dms')"

	Timeit for blocksize: 2**30: That was a mistake. It's definitely too big! Ditto for 2**28
	Timeit for blocksize: 3**26: 1 loop, best of 5: 53.4 sec per loop <--definitely faster than any other method! 
	Timeit for blocksize: 2**24: 1 loop, best of 5: 37.8 sec per loop <--GOING WITH 2**24!
	Timeit for blocksize: 2**22: 1 loop, best of 5: 43.1 sec per loop


	'''




def normalizeColumns(series):
	new=series.map_partitions(apply(json_dumps))
	new.map_partitions(apply(json_loads))
	return(new.map_partitions(apply(json_normalize)))
	# new=data.apply(json.dumps)
	# temp=json.dumps(data)
	# temp=json.loads(temp)
	# temp=json_normalize(temp)
	# return temp
	# new=dataframe['data'].apply(json.dumps)
	# dd.from_pandas(dataframe['data']).map_partitions(lambda df: df.apply(json.dumps)).compute(scheduler='processes')
	# dd.from_pandas(chunk['data'], npartitions=4).map_partitions(lambda df: df.apply(lambda row: json_normalize(json.loads(json.dumps(row))))).compute(scheduler='processes')

	return json_normalize(dataframe['data'].apply(json.loads(json.dumps)))


'''
I was on to something with the usingPANDAS. With that one, I found that the reading in was NOT the problem, it was the calculations. I also know that for small chunks, the calculations can be done much faster. So why not combine that?
	1) Read in with chunks like in usingPANDAS
	2) For each chunk, run the process, THEN WRITE OUT TO A PANDAS csv
	3) At the end, read in all of the csvs. (Or maybe just append to one csv )
	4) Find the duplicates
	5) DONE
	
	I could find the duplicates in each chunk, and in fact, that was my first thought, but that assumes that the json strings are sorted in a way that all of the repeats are near each other. More likely, the jsons are sorted by time or something, so there's no promise that I'll find all of the repeats. Better to just send out the whole chunk to file. 
'''
def readInOutIn(file):
	chunks=[]
	folder=path.join(getcwd(), "temp")
	if not path.isdir(folder):
		mkdir(folder)

	timing.log("Starting reading-in")
	reader=pd.read_json((file),lines=True, chunksize=100000, dtype=False)

	timing.log("Starting chunk processing")
	label=0 #for keeping track of the files. Could just do enumerate, but why get the length of reader? It could be huge. 
	for chunk in reader:
		new=chunk['data'].apply(json.dumps)
		new=json_normalize(new.apply(json.loads))

		for column in chunk.columns:
			if "data" is not column:
				new[column]=chunk[column]
		del new['data'] #remove data now because dictionary screws things up later.

		#NOW WRITE OUT!
		timing.log("Writing csv chunk %s"%label)
		new.to_csv(path.join(folder, "chunk_%s.csv"%label), chunksize=100000)
		label+=1

	timing.log("Reading back in!")
	files=glob(path.join(folder, "*.csv"))
	for f in files:
		new=pd.read_csv(file)
		chunks.append(new)

	#now convert the list of dataframes certsDF=pd.concat(chunks, ignore_index=True, sort=True)into a single dataframe
	certsDF=pd.concat(chunks, ignore_index=True, sort=True)

	timing.log("finding dups")
	dups=certsDF.duplicated(subset='leaf_cert.fingerprint')
	dups=certsDF[dups]
	timing.log("writing dups to file")
	dups.to_csv("duplicates_readInOutIn.csv")










def usingPANDAS(file):
	#set the size of the information to process at any time, so we're not sending it to memory
	chunks=[] #for storing all the chunks so that we can send them all to a merged dataframe later.
	certs=[]
	#PANDAS has a jsonreader that works with chunks! AND it works well with line-delimitors
	timing.log("Starting reading-in")
	reader=pd.read_json((file),lines=True, chunksize=100000, dtype=False) #so it doesn't infer the type?
	# print(reader)
	# chunks=(flattenDict(chunk.to_dict(), "",{}) for chunk in reader)
	# chunks=[chunk for chunk in reader]
	# certs.append([pd.DataFrame(json_normalize(x)) for x in chunks['data']])
	# certs.append([pd.DataFrame(json_normalize(x)) for x in chunk['data'] for chunk in reader])
	# certsDF = pd.concat([pd.DataFrame(json_normalize(x)) for x in chunk['data']],ignore_index=True)
	timing.log("Starting chunk processing")
	for chunk in reader:
		# print(chunk)
		# columns=chunk.columns
		new=chunk['data'].apply(json.dumps)
		new=json_normalize(new.apply(json.loads))
		# new=chunk['data'].apply(flattenDict, args={'',{}})
		# print("new: \n", new)
		# chunk=pd.concat([chunk, new], axis=1)#.to_dict()
		# chunk.merge(new, how="outer")
		# chunk=chunk.update(new)
		for column in chunk.columns:
			if "data" is not column:
				new[column]=chunk[column]
		del new['data'] #remove data now because dictionary screws things up later.

		# print("type(new): ", type(new))
		# print("data in set(new.columns): ", "data" in set(new.columns))
		# print("chunk.keys: ", chunk.keys())
		# print("chunk.columns: ", chunk.columns)
		# print("chunk: ", chunk)
		# print("new: ", new)
		# print("new.columns: ", new.columns)
		# for column in columns:
		# 	# chunk[column]=eval('json_normalize(chunk.{}.apply(json.loads))'.format(column))
		# 	# chunk.merge(chunk, eval('json_normalize(chunk.{}.apply(json.loads))'.format(column)))
		# 	# print(type(chunk[column]))
		# 	# if isinstance(chunk[column], dict):
		# 	# 	new=pd.Dataframe(chunk[column])
		# 	# 	chunk=pd.concat([chunk, new], axis=1)
		# 	# 	print("chunk columns: ", chunk.columns)
		# 	# try:
		# 	# print(type(chunk[column]))
		# 	# print(chunk[column])
		# 	# # print(chunk[column].to_dict().keys())
		# 	# # raise("break")
		# 	# new=pd.DataFrame(chunk[column].values)#to_dict())
		# 	# # print(new.columns)
		# 	# chunk=pd.concat([chunk, new], axis=1)
		# 	# print("chunk columns: ", chunk.columns)

		# 	# new=pd.value_counts(chunk[column])
		# 	new=chunk[column].apply(pd.DataFrame.from_dict)
		# 	print("new: ", new)
		# 	chunk=pd.concat([chunk, new], axis=1)

		# 	# except:
		# 		# raise(Error)

		# chunks.append(chunk)
		# print("new: \n", new )
		# new.to_csv("temp_new.csv")
		chunks.append(new)
		# timing.log("just finish a loop") #THIS SHOWS IT TAKES ~.06seconds to run a loop. 

		# certs=[pd.concat([pd.DataFrame(json_normalize(x)) for x in chunk['data']])]
		# print(chunk)
		# chunks.append(chunk)
		# chunks.append(certs)
	# print(certs[1])

		
	"""		
	NEW plan. Clearly the chunking is a good idea, but we still have to deal with the json strings. So let's try to normalize those AFTER. If the column contains strings that are valid jsons, convert them.
	Nope. Not working. We can't treat them like jsons, because they're going in as dictionaries. They're not acting like dictionaries, though. The data column is a series. I want to be able to extract the dictionaries and then attach them to the rest of the dataframe, so we preserve any columns that aren't the 'data'. 
	"""
		

	#now make a major dataframe 
	# certsDF=reduce(lambda x, y: pd.merge(x, y), chunks)
	# print(type(chunks))
	# print(len(chunks))
	# print("data in set(new.columns): ", "data" in set(new.columns))
	# print("set(new.columns): ", set(new.columns))
	# certsDF=pd.DataFrame(chunks)
	timing.log("Starting Concat")
	certsDF=pd.concat(chunks, ignore_index=True, sort=True)
	# certsDF=reduce(lambda x, y: pd.merge(x, y), chunks)
	# print(certsDF.shape)
	# certsDF=pd.DataFrame(certs)
	# print(certsDF.columns)
	timing.log("finding dups")
	dups=certsDF.duplicated(subset='leaf_cert.fingerprint')
	dups=certsDF[dups]
	timing.log("writing dups to file")
	dups.to_csv("duplicates_PANDAS.csv")

	'''
	python -m timeit "from searchDuplicateCrts import usingPANDAS; usingPANDAS('/Users/esbailin/Desktop/future/bailin/ctl_records_subsample')" --> 10377 rows

	Timeit for 100 chunks: 1 loop, best of 5: 6.7 sec per loop
	Timeit for 1000 chunks: 1 loop, best of 5: 5.73 sec per loop
	Timeit for 10000 chunks: 1 loop, best of 5: 5.85 sec per loop

	python -m timeit "from searchDuplicateCrts import usingPANDAS; usingPANDAS('/Users/esbailin/Desktop/future/bailin/subsample2.dms')" --> 22256
	
	Timeit for 1000 chunks: 1 loop, best of 5: 12.5 sec per loop
	Timeit for 10000 chunks: 1 loop, best of 5: 13.1 sec per loop
	Timeit for 100000 chunks: 1 loop, best of 5: 13.7 sec per loop

	python -m timeit "from searchDuplicateCrts import usingPANDAS; usingPANDAS('/Users/esbailin/Desktop/future/bailin/subsample3.dms')" --> 125225

	Timeit for 1000 chunks: 1 loop, best of 5: 75.1 sec per loop
	Timeit for 10000 chunks: 1 loop, best of 5: 71.2 sec per loop #HUGELY! BETTER THAN PREVIOUS METHODS!!!
	Timeit for 100000 chunks: 1 loop, best of 5: 94.2 sec per loop #So clearly, less than 100000

	:::> With these results, it seems that 100000 is sufficient for the huge file, FOR now. I should run timeits on the huge file, but I don't know if I have time for that. I'll just use the embedded timing method to calculate how long it takes to run the big one. Here's to hoping for less than 2hours! 
	'''


def usingIjson(file):
	# file="/home/lnvp-linux-wkst1/Desktop/future/ctl_records_subsample"

	certs=[] # for holding all of the ordered data
	#from https://stackoverflow.com/questions/37200302/using-python-ijson-to-read-a-large-json-file-with-multiple-json-objects
	with open(file, encoding="UTF-8") as json_file:
	    # cursor = 0
	    for line_number, line in enumerate(json_file):
	    # for line in enumerate(json_file):
	        # print ("Processing line", line_number + 1,"at cursor index:", cursor)
	        line_as_file = io.StringIO(line)
	        # Use a new parser for each line
	        json_parser = ijson.parse(line_as_file)
	        cert={}
	        # print("json_parser: ", json_parser)
	        for prefix, kind, value in json_parser:
	            # print ("prefix=",prefix, "type=",kind, "value=",value)
	            if "string" is kind:
	            	cert.update({prefix:value})
        	certs.append(cert)
	        # cursor += len(line)

	certsDF=pd.DataFrame(certs)
	# print("dim(certsDF): ", certsDF.shape)
	# print("data.columns: ", certsDF.columns)
	# certsDF.to_csv("subsample_ijson_pd.csv")

	# print()
	dups=certsDF.duplicated(subset='data.leaf_cert.fingerprint')
	dups=certsDF[dups]
	# print(dups)
	# dups.to_csv("subsample_ijson_DUPS.csv")

def usingJson(file):
	#get file:
	# file="/home/lnvp-linux-wkst1/Desktop/future/ctl_records_subsample"

	certs=[] # for holding all of the ordered data
	with open(file, "r") as f:
		for line in f:
			# print(line)
			# cert=sortDictLists(prepRow(line))#, '', {})
			cert=flattenDict(json.loads(line), '', {})#, '', {})

			#we need to put the strings from sortDict back into dicts. 
			#MAYBE THIS ISN'T NECESSARY?!
			cs={}
			for c in cert:
				# print("c: ", c)
				cs.update({c[0]: c[1]})
			# print("*****type: ", type(cert))
			# print("cert: ",cert)
			# certs.append(cs)
			certs.append(cert)

	certsDF=pd.DataFrame(certs)
	# print("dim(certsDF): ", certsDF.shape)
	# print("data.columns: ", certsDF.columns)
	# certsDF.to_csv("subsample_ijson_pd.csv")

	# print()
	dups=certsDF.duplicated(subset='data.leaf_cert.fingerprint')
	dups=certsDF[dups]
	# print(dups)
	# dups.to_csv("subsample_json_DUPS.csv")

def usingJson_v2(file):
	#get file:
	# file="/home/lnvp-linux-wkst1/Desktop/future/ctl_records_subsample"

	# certs=[] # for holding all of the ordered data
	cs=[]
	with open(file, "rb") as f:
		certs=(json.loads(line) for line in f) #make generator, don't store in memory directly. 
	
		for cert in certs:
			cert=flattenDict(cert, '', {})
			cs.append(cert)

	# certsDF=pd.DataFrame(certs)
	certsDF=pd.DataFrame(cs) 

	# print("dim(certsDF): ", certsDF.shape)
	# print("data.columns: ", certsDF.columns)
	# certsDF.to_csv("subsample_ijson_pd.csv")

	# print()
	dups=certsDF.duplicated(subset='data.leaf_cert.fingerprint')
	dups=certsDF[dups]
	# print(dups)
	# dups.to_csv("subsample_json_DUPS.csv")
	dups.to_csv("duplicates.csv")

'''
Confirm ijson is worth it
'''
def timeMethods():
	setup="from searchDuplicateCrts import usingJson, usingIjson, usingJson_v2"

	# file="/home/lnvp-linux-wkst1/Desktop/future/ctl_records_subsample"
	# # print("usingIjson (sample1): ", timeit.timeit(setup=setup, stmt='usingIjson("{}")'.format(file), number=10))
	# print("usingJson (sample1): ", timeit.timeit(setup=setup, stmt='usingJson("{}")'.format(file), number=10))
	# print("usingJson_v2 (sample1): ", timeit.timeit(setup=setup, stmt='usingJson_v2("{}")'.format(file), number=10))

	# file="/home/lnvp-linux-wkst1/Desktop/future/subsample2"
	# # print("usingIjson (sample2): ", timeit.timeit(setup=setup, stmt='usingIjson("{}")'.format(file), number=10))
	# print("usingJson (sample2): ", timeit.timeit(setup=setup, stmt='usingJson("{}")'.format(file), number=10))
	# print("usingJson_v2 (sample2): ", timeit.timeit(setup=setup, stmt='usingJson_v2("{}")'.format(file), number=10))

	file="/home/lnvp-linux-wkst1/Desktop/future/subsample3"
	# print("usingIjson (sample3): ", timeit.timeit(setup=setup, stmt='usingIjson("{}")'.format(file), number=10))
	print("usingJson (sample3): ", timeit.timeit(setup=setup, stmt='usingJson("{}")'.format(file), number=3))
	print("usingJson_v2 (sample3): ", timeit.timeit(setup=setup, stmt='usingJson_v2("{}")'.format(file), number=3))

	'''
	#RESULTS OF FIRST RUN:
	usingIjson (sample1):  35.64337776298635
	usingJson (sample1):  8.808945185039192
	usingIjson (sample2):  76.70800173003227
	usingJson (sample2):  19.17413446609862
	usingIjson (sample3):  459.6714284690097
	usingJson (sample3):  114.13637442095205
	usingIjson (sample1):  37.192120782099664
	usingJson (sample1):  8.95059080189094
	'''
	'''
	#RESULTS OF SECOND TEST
	python -m timeit 'from searchDuplicateCrts import timeMethods; timeMethods()'
	usingJson (sample1):  9.268286403967068
	usingJson_v2 (sample1):  8.841814633924514
	usingJson (sample2):  20.21853825217113
	usingJson_v2 (sample2):  19.230859401170164
	usingJson (sample3):  117.22211304213852
	usingJson (sample3):  114.69562518619932
	usingJson (sample1):  9.328198273899034
	usingJson_v2 (sample1):  8.91312710288912
	usingJson (sample2):  20.411472880048677
	usingJson_v2 (sample2):  19.404500395059586
	usingJson (sample3):  119.32550774584524
	usingJson (sample3):  114.58662933087908
	usingJson (sample1):  9.222406175918877
	usingJson_v2 (sample1):  8.904197013936937
	usingJson (sample2):  20.289959273999557
	usingJson_v2 (sample2):  19.57311406219378
	usingJson (sample3):  118.5590409648139
	usingJson (sample3):  114.34158619190566
	usingJson (sample1):  9.123866918031126
	usingJson_v2 (sample1):  8.736241657054052
	usingJson (sample2):  20.1273577590473
	usingJson_v2 (sample2):  19.43689747690223
	usingJson (sample3):  117.52145659504458
	usingJson (sample3):  109.31005174899474
	usingJson (sample1):  8.60072591691278
	usingJson_v2 (sample1):  8.234143032925203
	usingJson (sample2):  19.113397310953587
	usingJson_v2 (sample2):  18.396775020984933
	usingJson (sample3):  113.17863137694076
	usingJson (sample3):  109.97878070408478
	usingJson (sample1):  8.486295140814036
	usingJson_v2 (sample1):  8.22266330011189
	usingJson (sample2):  18.949718659976497
	usingJson_v2 (sample2):  18.359669177094474
	usingJson (sample3):  112.9216108971741
	usingJson (sample3):  109.39436202519573
	usingJson (sample1):  8.457444502972066
	usingJson_v2 (sample1):  8.218067151959985
	usingJson (sample2):  18.962617667857558
	usingJson_v2 (sample2):  18.256325237220153
	usingJson (sample3):  112.87163304304704
	usingJson (sample3):  109.33912342484109
	usingJson (sample1):  8.482060621026903
	usingJson_v2 (sample1):  8.145873442059383
	usingJson (sample2):  18.94858014700003
	usingJson_v2 (sample2):  18.146990463137627
	usingJson (sample3):  113.79034738009796
	usingJson (sample3):  109.21794279292226
	usingJson (sample1):  8.597782640950754
	usingJson_v2 (sample1):  8.205268741119653
	usingJson (sample2):  18.970948527101427
	usingJson_v2 (sample2):  18.213877220870927
	usingJson (sample3):  113.35747700300999
	usingJson (sample3):  109.31005109893158
	usingJson (sample1):  8.425198971992359
	usingJson_v2 (sample1):  8.136808255920187
	usingJson (sample2):  18.775070013012737
	usingJson_v2 (sample2):  17.891853244043887
	usingJson (sample3):  112.99325982504524
	usingJson (sample3):  109.41139507107437
	usingJson (sample1):  8.618158485973254
	usingJson_v2 (sample1):  8.19867969607003
	usingJson (sample2):  18.642994106048718
	usingJson_v2 (sample2):  17.99040506198071
	usingJson (sample3):  113.06386271095835
	usingJson (sample3):  108.85116258100607
	usingJson (sample1):  8.55431603291072
	usingJson_v2 (sample1):  8.194189763860777
	usingJson (sample2):  18.64476641616784
	usingJson_v2 (sample2):  17.85302364989184
	usingJson (sample3):  112.94438758003525
	usingJson (sample3):  108.71583245391957
	usingJson (sample1):  8.726059461012483
	usingJson_v2 (sample1):  8.253968856995925
	usingJson (sample2):  18.710836203070357
	usingJson_v2 (sample2):  18.013195749837905
	usingJson (sample3):  113.29407830303535
	usingJson (sample3):  108.95358220604248
	usingJson (sample1):  8.441362912068143
	usingJson_v2 (sample1):  8.099830167135224
	usingJson (sample2):  18.593231353908777
	usingJson_v2 (sample2):  17.86932290182449
	usingJson (sample3):  113.09821366588585
	usingJson (sample3):  108.73518713703379
	usingJson (sample1):  8.463629209902138
	usingJson_v2 (sample1):  8.117978785885498
	usingJson (sample2):  18.42898819106631
	usingJson_v2 (sample2):  17.80342112411745
	usingJson (sample3):  112.90247440198436
	usingJson (sample3):  109.04040180402808
	usingJson (sample1):  8.47171609615907
	usingJson_v2 (sample1):  8.138799960026518
	usingJson (sample2):  18.452345600817353
	usingJson_v2 (sample2):  17.71188673004508
	usingJson (sample3):  112.36971413204446
	usingJson (sample3):  109.03683636221103
	usingJson (sample1):  8.40954012516886
	usingJson_v2 (sample1):  8.115414389176294
	usingJson (sample2):  18.425362571142614
	usingJson_v2 (sample2):  17.78201344399713
	usingJson (sample3):  112.33800739981234
	usingJson (sample3):  108.68973794393241
	usingJson (sample1):  8.464604338863865
	usingJson_v2 (sample1):  8.09839853621088
	usingJson (sample2):  18.449338108999655
	usingJson_v2 (sample2):  17.672893785871565
	usingJson (sample3):  112.71247678785585
	usingJson (sample3):  108.91564979613759
	usingJson (sample1):  8.458330741152167
	usingJson_v2 (sample1):  8.13037392986007
	usingJson (sample2):  18.46901690401137
	usingJson_v2 (sample2):  17.84113406110555
	usingJson (sample3):  112.68664718884975
	usingJson (sample3):  108.72933574602939
	usingJson (sample1):  8.537419356871396
	usingJson_v2 (sample1):  8.20236833114177
	usingJson (sample2):  18.334909196011722
	usingJson_v2 (sample2):  17.732283795950934
	usingJson (sample3):  112.4451036839746
	usingJson (sample3):  108.78542139893398
	usingJson (sample1):  8.384103484917432
	usingJson_v2 (sample1):  8.104602935956791
	usingJson (sample2):  18.421104457927868
	usingJson_v2 (sample2):  17.766838105162606
	usingJson (sample3):  112.59477828489617
	usingJson (sample3):  108.54388658609241
	usingJson (sample1):  8.457705494016409
	usingJson_v2 (sample1):  8.107315958011895
	usingJson (sample2):  18.408782438840717
	usingJson_v2 (sample2):  17.796526334946975
	usingJson (sample3):  112.81448833900504
	usingJson (sample3):  109.02163033001125
	usingJson (sample1):  8.495913784019649
	usingJson_v2 (sample1):  8.167313264217228
	usingJson (sample2):  18.486214217031375
	usingJson_v2 (sample2):  17.590045480057597
	usingJson (sample3):  112.55883460096084
	usingJson (sample3):  108.7615531210322
	usingJson (sample1):  8.471582181984559
	usingJson_v2 (sample1):  8.09589281398803
	usingJson (sample2):  18.564955228939652
	usingJson_v2 (sample2):  17.842915607150644
	usingJson (sample3):  112.42266227910295
	usingJson (sample3):  109.14612703910097
	usingJson (sample1):  8.463388126110658
	usingJson_v2 (sample1):  8.031285314122215
	usingJson (sample2):  18.480005885940045
	usingJson_v2 (sample2):  17.70272097713314
	usingJson (sample3):  112.87815855303779
	usingJson (sample3):  108.77135197306052
	usingJson (sample1):  8.457728903042153
	usingJson_v2 (sample1):  8.064292408991605
	usingJson (sample2):  18.39129592315294
	usingJson_v2 (sample2):  17.97003356809728
	usingJson (sample3):  112.96159790991805
	usingJson (sample3):  108.90647747600451
	usingJson (sample1):  8.444802372017875
	usingJson_v2 (sample1):  8.240135686006397
	usingJson (sample2):  18.450528791174293
	usingJson_v2 (sample2):  17.654063532827422
	usingJson (sample3):  112.74505742499605
	usingJson (sample3):  108.4718284860719
	usingJson (sample1):  8.433507841080427
	usingJson_v2 (sample1):  8.120567898033187
	usingJson (sample2):  18.393228549975902
	usingJson_v2 (sample2):  17.773249733028933
	usingJson (sample3):  112.78254036488943
	usingJson (sample3):  109.32410072302446
	usingJson (sample1):  8.37075019790791
	usingJson_v2 (sample1):  8.199759071925655
	usingJson (sample2):  18.38781941612251
	usingJson_v2 (sample2):  17.781014570966363
	usingJson (sample3):  112.91855630395003
	usingJson (sample3):  108.97876754403114
	usingJson (sample1):  8.53962426004
	usingJson_v2 (sample1):  8.160044739022851
	usingJson (sample2):  18.380576393799856
	usingJson_v2 (sample2):  17.869484993861988
	usingJson (sample3):  112.84630944300443
	usingJson (sample3):  108.72713151411153
	usingJson (sample1):  8.357039446011186
	usingJson_v2 (sample1):  8.130624305922538
	usingJson (sample2):  18.31611322099343
	usingJson_v2 (sample2):  17.664012799039483
	usingJson (sample3):  112.47885200986639
	usingJson (sample3):  108.96794932801276
	usingJson (sample1):  8.429081305162981
	usingJson_v2 (sample1):  8.211195413023233
	usingJson (sample2):  18.350082270102575
	usingJson_v2 (sample2):  17.67571084899828
	usingJson (sample3):  112.605528148124
	usingJson (sample3):  108.55814584088512
	usingJson (sample1):  8.575240462087095
	usingJson_v2 (sample1):  8.163357200101018
	usingJson (sample2):  18.412766295019537
	usingJson_v2 (sample2):  17.69811917981133
	usingJson (sample3):  112.73721452895552
	usingJson (sample3):  109.08874585200101
	usingJson (sample1):  8.439677791902795
	usingJson_v2 (sample1):  8.109200898790732
	usingJson (sample2):  18.387274252949283
	usingJson_v2 (sample2):  17.7954245058354
	usingJson (sample3):  112.57343062991276
	usingJson (sample3):  108.89170281006955
	usingJson (sample1):  8.428776105167344
	usingJson_v2 (sample1):  8.13118127384223
	usingJson (sample2):  18.454236933030188
	usingJson_v2 (sample2):  17.88526326487772
	usingJson (sample3):  112.78249149792828
	usingJson (sample3):  109.04251154488884
	usingJson (sample1):  8.521719304844737
	usingJson_v2 (sample1):  8.1893800010439
	usingJson (sample2):  18.451002869056538
	usingJson_v2 (sample2):  17.770314262947068
	usingJson (sample3):  112.39034603303298
	usingJson (sample3):  108.61503490502946
	usingJson (sample1):  8.463443842949346
	usingJson_v2 (sample1):  8.203809785889462
	usingJson (sample2):  18.501847438979894
	usingJson_v2 (sample2):  17.7243826771155
	usingJson (sample3):  112.31008779094554
	usingJson (sample3):  109.06321070413105
	usingJson (sample1):  8.446885549928993
	usingJson_v2 (sample1):  8.158500131918117
	usingJson (sample2):  18.390855035046116
	usingJson_v2 (sample2):  17.7209776150994
	usingJson (sample3):  113.14698154199868
	usingJson (sample3):  108.84900826704688
	usingJson (sample1):  8.429142431821674
	usingJson_v2 (sample1):  8.200061745941639
	usingJson (sample2):  18.311052242061123
	usingJson_v2 (sample2):  17.817597785033286
	usingJson (sample3):  113.45475183404051
	usingJson (sample3):  109.04837727709673
	usingJson (sample1):  8.535036253044382
	usingJson_v2 (sample1):  8.106813685037196
	usingJson (sample2):  18.301348564913496
	usingJson_v2 (sample2):  17.694638417102396
	usingJson (sample3):  113.56961958203465
	usingJson (sample3):  109.0831012269482
	10 loops, best of 3: 274 sec per loop
	
	#Seems like usingJson_v2 wins for the larger files. It's the one that is creating a lazy generator instead of reading in to memory.  
	'''



if __name__=="__main__": 
    main(sys.argv[1:]) #take all the arguments after the name of the script









