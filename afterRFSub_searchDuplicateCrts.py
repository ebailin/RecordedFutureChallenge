#!/usr/bin/env python3

"""
This is my attempt to finish the challenge, despite having already submitted my code. 
"""

import json
import sys
import timeit  # testing purposes
import timing  # for complete script timings. Taken from an answer on stackoverflow years ago. NOT MINE
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from os import path, getcwd, mkdir
from pandas.io.json import json_normalize

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


