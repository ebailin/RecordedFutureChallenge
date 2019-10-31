#!/usr/bin/env python3

"""
This is my attempt to finish the challenge, despite having already submitted my code. 
"""

import json
import sys
import timeit  # testing purposes
import timing  # for complete script timings. Taken from an answer on stackoverflow years ago. NOT MINE
<<<<<<< HEAD
import dask.delayed as dl
import dask.dataframe as dd
import pandas as pd
import sqlalchemy as sql
from dask.distributed import Client
from dask import compute
from os import path, getcwd, mkdir
from io import StringIO
from pandas.io.json import json_normalize
# from mysql.connector import MySQLConnection, Error

from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
=======
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from os import path, getcwd, mkdir
from pandas.io.json import json_normalize
>>>>>>> 315182e645bb7447bdc4375487f4aa133cd2b7bf

"""
When we last left off, I was still trying to get dask to work. I've since concluded that my setup was good, but the compute() method was the holdup. I thought there was no solution to the compute issue,but then I found 
"""
# Noted copied from https://stackoverflow.com/questions/24448543/how-would-i-flatten-a-nested-dictionary-in-python-3
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

def usingDASKClient(file):
	client=Client(memory_limit='5GB', processes=True, threads_per_worker=4, n_workers=1) #so we can use distributed
	client.restart()
	chunks = []
	timing.log("Starting reading-in")

	reader = dd.read_json(
        file,
        lines=True,
        blocksize=2 ** 28,
        meta={"data": object, "message_type": object},
    )
    # t=reader['data'].map_partitions(lambda df: df.apply(lambda x: x.apply(flattenDict, key='', result={}))).to_bag()
    # t=reader.map_partitions(lambda df: df['data'].apply(flattenDict, key='', result={})).to_bag()
	datas = (
        reader["data"]
        .map_partitions(lambda df: df.apply((lambda row: flattenDict(row, "", {}))))
        .to_bag()
    )
	new = datas.to_dataframe()
	new["message_type"] = reader["message_type"]
	'''
    noDups=new.drop_duplicates(subset='leaf_cert.fingerprint')
    #do left-join to get all the values in new that aren't in noDups:
    dups=new.join(noDups,  how='left')
    dups.to_csv(path.join(getcwd(), "temp_dups.csv"))
	'''
	timing.log("Starting compute")
	new = new.compute()
	dups = new.duplicated(subset="leaf_cert.fingerprint")
	dups = new[dups]
    # dups.to_csv("duplicates_DASK.csv")

"""
That's a no-go for the client. It keeps running out of memory. I don't think this computer has enough spare memory to split. So let's move to a completely different foot. Let's try mysql loading.
"""

<<<<<<< HEAD
"""
With this attempt, I'm going to try to read everything in with dask and send it chunk-wise into MySQL.

After attempting to use MySQL and running into an issue inserting chunks, I'm going to follow the tutorial here: https://towardsdatascience.com/how-to-handle-large-datasets-in-python-with-pandas-and-dask-34f43a897d55. I wasn't going to use the copy_from, but it seems like it's a good idea given that we know the data is already validated and we just want to shove it in. Unfortunately, the copy_from method is only from pyscopg2, which means I'll need to switch to postgresSQL. 
	OR
	I could just use sqlalchemy's sessions and bulk insert? Let's try that first so  I don't have to switch to postgresSQL.--> Not worth it. The session method, while nice, requires that we have a pre-determined base, i.e. a table, already in the database. We don't. UNLESS, perhaps I use the first method to create the table, take the mapping, then use it for the rest? Is that worth it? Let's see. 
"""

def usingMySQL(file):
	# file='/home/lnvp-linux-wkst1/Desktop/future/ctl_records_subsample' 

	timing.log("Setting Up MySQL")
	#MySQL setup
	#I probably should do this with another method and a config file, but this is a test anyway. I'm breaking protocol just to get it done. 
	info=pd.read_csv(path.join(getcwd(), "private.csv"))
	engine=sql.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}?charset=utf8mb4'.format(info['user'][0], info['password'][0], info['host'][0], info['database'][0])) #maybe try with fast_executemany if need to use pyodbc
	# conn=sql.connect(user=info['user'][0], password=info['password'][0], host=info['host'][0], dbname=info['database'][0])

	#necessary for postgre, but not MySQl session
	# conn=engine.raw_connection() #so we can get a cursor, which we need later? (according to https://towardsdatascience.com/how-to-handle-large-datasets-in-python-with-pandas-and-dask-34f43a897d55?)
	# cur=conn.cursor()

	conn=engine.connect()
	errored=[] #tables that don't cooperate

	timing.log("Starting read-in")
	reader = dd.read_json(
        file,
        lines=True,
        blocksize=2 ** 28,
        meta={"data": object, "message_type": object},
    )
	datas = (
        reader["data"]
        .map_partitions(lambda df: df.apply((lambda row: flattenDict(row, "", {}))))
        .to_bag()
    )
	timing.log("Making new")
	new = datas.to_dataframe().fillna(value="None") #for padding later during the insert

    #added after realized issue with the chunk to sql. See note there. 
	timing.log("Changing dtypes in new")
	for k,v in dict(new.dtypes).items():
		if v != "object":
			new[k]=new[k].astype(str)
	new["message_type"] = reader["message_type"]

	timing.log("Making table in db")
	#make empty dataframe for creating an empty table
	pd.DataFrame(columns=new.columns).to_sql('crts', con=engine, if_exists='replace', index=False)

	#need to set primary key for the mapping:
	with engine.connect() as con:
		con.execute('ALTER TABLE crts ADD id SERIAL PRIMARY KEY;')

	timing.log("Making mappings")
	#now get the mapping from the table created above?
	meta=MetaData(bind=engine)
	meta.reflect(engine)
	Base=automap_base(metadata=meta)
	Base.prepare()
	Cert=Base.classes.crts #the cert class




	'''
	PRE SESSIONMAKER ATTEMPT (runs in to a 'cursor object no attribute copy_from' error, which I THINK is from the format of the data? Just a guess.)
	for n in range(new.npartitions):
		chunk=new.get_partition(n).compute()
		output=StringIO()
		chunk.to_csv(output, sep=',', header=False, index=False)
		output.seek(0)
		try:
			cur.copy_from(output, 'certs', null=" ")
		except Exception as error:
			print(error)
			errored.append(chunk)
			conn.rollback()
			continue
		conn.commit()
	'''

	#Post session-maker addition.
	Session=sessionmaker(bind=conn)
	session=Session()
	timing.log("starting bulk loading!")
	for n in range(new.npartitions):
		chunk=new.get_partition(n).applymap(str).compute()#[0:1000]#now this worked, so why doesn't it always?
		# chunk=new.get_partition(n).compute().head() #this doesn't, so we do need to convert to strings.
		#LOOKING at the data, it appears that the 'seen' column USUALLY contains a number, but sometimes something gets off and then it contains a string. With this in mind, I should only need to make the seen column a string, rather than everything. This would also explain why the 'head()' gets inserted just fine, but not the whole thing.
		#ACTUALLY, it appears that the issue comes from any column that's labeled as not a string! In a few rows, it seems that all of the data is offset by one or two. This is something wrong with the initial json, which I need to  check later. For now, if the column type isn't a string, make it one:
		#Why is this here? Moving it up to before the loop so we only have to do it once.
		# for k,v in dict(chunk.dtypes).items():
		# 	if v != "object":
		# 		chunk[k]=chunk[k].astype(str)

		output=StringIO()
		session=Session()
		session.bulk_insert_mappings(Cert, chunk.to_dict(orient="records"))
		# session.bulk_insert_mappings(Cert, chunk.to_dict())
		session.commit()
		session.close()
		# chunk.to_csv(output, sep=',', header=False, index=False)
		# output.seek(0)
		# try:
		# 	cur.copy_from(output, 'certs', null=" ")
		# except Exception as error:
		# 	traceback.print_exc(error)
		# 	errored.append(chunk)
		# 	conn.rollback()
		# 	continue
		# conn.commit()


	timing.log("Starting the sql computations")
	#now do the computation!
	# query='SELECT *, COUNT(`leaf_cert.fingerprint`) FROM crts GROUP BY `leaf_cert.fingerprint` HAVING COUNT(`leaf_cert.fingerprint`)>1'
	# query='SELECT * FROM crts GROUP BY `leaf_cert.fingerprint` HAVING COUNT(`leaf_cert.fingerprint`)>1'
	# query="SELECT * FROM crts WHERE `leaf_cert.fingerprint` IN (SELECT `leaf_cert.fingerprint` FROM crts GROUP BY `leaf_cert.fingerprint` HAVING COUNT(`leaf_cert.fingerprint`)>1)" #This takes WAYYY too long.
	query="SELECT cert1.* FROM crts AS cert1 JOIN (SELECT `leaf_cert.fingerprint` FROM crts GROUP BY `leaf_cert.fingerprint` HAVING COUNT(`leaf_cert.fingerprint`) > 1) AS cert2 ON cert1.`leaf_cert.fingerprint` = cert2.`leaf_cert.fingerprint`"

	# dups=engine.execute(query).fetchall()
	# dups=pd.DataFrame(dups, columns=new.columns)

	#read straight from MySQL into new pandas for fast dumping to csv (and we hope that it's small enough for memory...)
	dups=pd.read_sql(query, engine)
	timing.log("Dumping excess duplicates (JUST IN CASE)")
	dups=dups[dups.duplicated(subset="leaf_cert.fingerprint")] #ADDED THE DUPLICATED CHECK B/C ASSUMING that we only want one example
	timing.log("Writing out!")
	dups.to_csv(path.join(getcwd(), "usingMySQL_dups.csv"))

	'''
	dto_sql=dl(pd.DataFrame.to_sql)	
	out = [dto_sql(d, 'certs', engine, if_exists='append', index=True) for d in new.to_delayed()]
	compute(*out)
	'''
	'''

	table=("CREATE TABLE 'crts' ("

		") ENGINE=InnoDB")
	try:  
		print('Connecting to MySQL database...')
		conn = MySQLConnection(host=info['host'][0], database=info['database'][0], user=info['user'][0], password=info['password'][0])
		if conn.is_connected():
			print("Connection established")
		else:
			print("Connection failed! ")
	except Error as error: 
		print(error)
	finally:
		if conn is not None and conn.is_connected():
			conn.close()
			print("Connection closed...")
	'''
	'''
	This method is definitely slower:

	python -m timeit "from afterRFSub_searchDuplicateCrts import usingMySQL; usingMySQL('/home/lnvp-linux-wkst1/Desktop/future/subsample2')" : 10 loops, best of 3: 43.5 sec per loop
	
	BUT, this is the first time timeit did 10 loops. That has to mean something, right? It had enough variation to require 10 loops instead of 1. I'm going to proceed as if it weren't 30+ seconds slower than the usingDASK method and trying it with the subsample3. Maybe it won't take a full 30 minutes to give a time...
	It's important to note that the computations appear to take about 2 seconds or so with the 1GB file. Once again, it's the loading that's the problem.

	Got a memory block overload trying to do timeit with the 1GB. I'm really hoping that's just because of the number of times timeit ran? 

	Looking at just my time function, it took 4 minutes and 44 seconds to do the 1GB. It's official that usingMySQL is the slowest method so far for the little files, BUT, the methods that were very fast for the subfiles crashed the big one, so maybe it's worth running it on the 31GB? It's late enough that I'll just let it run. If it crashes, I tried. 


	22 hours and 47 minutes later:

	IT WORKED!!!!
	
	Here are the logs:
	(future) (base) lnvp-linux-wkst1:~/Desktop/future/bailin$ python -c "from afterRFSub_searchDuplicateCrts import usingMySQL; usingMySQL('/home/lnvp-linux-wkst1/Desktop/future/ctl_records_sample.jsonlines')"
	========================================
	2019-10-29 20:50:40 - Start Program
	========================================

	========================================
	2019-10-29 20:50:40 - Setting Up MySQL
	========================================

	========================================
	2019-10-29 20:50:41 - Starting read-in
	========================================

	========================================
	2019-10-29 20:50:41 - Making new
	========================================

	========================================
	2019-10-29 20:50:47 - Changing dtypes in new
	========================================

	========================================
	2019-10-29 20:50:47 - Making table in db
	========================================

	========================================
	2019-10-29 20:50:49 - Making mappings
	========================================

	========================================
	2019-10-29 20:50:49 - starting bulk loading!
	========================================

	========================================
	2019-10-29 22:57:03 - Starting the sql computations
	========================================

	========================================
	2019-10-30 19:37:33 - Dumping excess duplicates (JUST IN CASE)
	========================================

	========================================
	2019-10-30 19:37:33 - Writing out!
	========================================

	========================================
	2019-10-30 19:37:47 - End Program
	Elapsed time: 22:47:06.996887
	========================================



	'''



=======
>>>>>>> 315182e645bb7447bdc4375487f4aa133cd2b7bf

