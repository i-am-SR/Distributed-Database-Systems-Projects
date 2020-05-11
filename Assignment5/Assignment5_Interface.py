#
# Assignment5 Interface
# Name: Sumit Rawat
#
from math import sin, cos, sqrt, pow, atan2, radians
from pymongo import MongoClient
import os
import sys
import json

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
	cityToSearch = '^' + cityToSearch + '$'
	#ensuring that the whole string is matched i.e. cityToSearch is not a substring of the macthed string
	records = collection.find({'city': {'$regex': cityToSearch, '$options': '-i'}})
	#making the regex case insensitive
	with open(saveLocation1, "w") as file:
		for record in records:
			file.write(record['name'].upper() + "$" + record['full_address'].upper() + "$" + record['city'].upper() + "$" + record['state'].upper() + "\n")

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
	records = collection.find({'categories': {'$in': categoriesToSearch}})
	myLatitude = float(myLocation[0])
	myLongitude = float(myLocation[1])
	with open(saveLocation2, "w") as file:
		for record in records:
			latitude = float(record['latitude'])
			longitude = float(record['longitude'])
			if DistanceFunction(latitude, longitude, myLatitude, myLongitude) <= maxDistance:
				file.write(record['name'].upper() + "\n")

def DistanceFunction(lat2, lon2, lat1, lon1):
    R = 3959
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    deltaphi = radians(lat2 - lat1)
    deltalambda = radians(lon2 - lon1)
    a = pow(sin(deltaphi / 2), 2) + (cos(phi1) * cos(phi2) * pow(sin(deltalambda / 2), 2))
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    d = R * c
    return d
