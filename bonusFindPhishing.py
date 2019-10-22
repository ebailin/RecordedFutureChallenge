#! /usr/bin/env python3

"""
Written by: Emma Bailin
Created on: October 18, 2019
Last Modified: NA
Description: Takes in a csv of duplicate certificate log data entries and identifies phishing areas 
"""
"""
Initial thoughts:
	Known:
		1) Looking for "Top-Level Domains" (TLD)
		2) Phishing domains often appear to be variations on targets (eg: paypal --> paypall)
		2) We need to dentify these "bad eggs"
			Perhaps using something like the fuzzyword package could help us find domains that look similiar. That could identify word variations. 
				Pros to fuzzyword (or the like):
					1) It'll find all variations of the top websites (e.g. apple, paypal) if we know them AD-HOC
				Cons:
					1) There are a potentially a _ton_ of domains that sound/look pretty similar.
					2) It'll be very inefficient to search every colun, so I'll have to know where to look. (But I'm guessing that 'cert_link' is probably a good place to start...)
					3) Need to know the possible targets before hand. 
		3) cert_link looks like the url and TLD are the ".com" portion of the urls. 
		4) As we need to look at urls, a url parser may be a good idea? That way we can automatically exclude anything like .com from the fuzzyword search. That'll at least cut down on some hits. BUT some phishers have "nested keywords". Maybe the solution there is to check the number of dots? Let's put that on the burner. At least counting the dots is faster than running through fuzzywords. 
			--> yes, urllib.parser can split the url so we can access jut the netloc. 
"""
import pandas as pd
from urllib.parse import urlparse
from os import path, getcwd

def extractNetloc(url):
	return urlparse(url)[1]

#based on the assumption that a nested loc will have a lot of periods
def countNestsLoc(netloc):
	return netloc.count(".")



#just read in the relevant (I'm assuming) column
data=pd.read_csv(path.join(getcwd(), "duplicates_PANDAS.csv"), usecols="cert_link")

#get the url
data['netloc']=data['cert_link'].apply(extractNetloc) #need to make function so we can just get the netloc value

data['counts']=data['netloc'].apply(countNestsLoc)

#MAKING ANOTHER ASSUMPTION HERE: phishing TLDs are less common than regular IPs, so if we get the average number of periods in the location, we can see the ones that are "highly nested"
meanDot=round(data['counts'].mean()) #round down
#extract those entries who have excessive dots:
excessUrl=data[data['counts'] > meanDot]

#make the counts a factor so we can easily bin the results
excessUrl['counts'].astype('category')

#look at numbers within each bin. Look for the anomilously high counts 
#(maybe get average in each bin and select the top quartile or something?)







