#
# Assignment5 Interface
# Name: Molife Chaplain
#

from pymongo import MongoClient
import os
import sys
import json
import math
import re

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    results = collection.find({"city":re.compile(cityToSearch,re.IGNORECASE)})
    res = open(saveLocation1, 'w')
    for each in results:
        each["full_address"] = each["full_address"].replace("\n",", ")
        res.write(each["name"].upper()+"$"+each["full_address"].upper()+"$"+each["city"].upper()+"$"+each["state"].upper())
        res.write("\n")
    res.close()
    


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    results = collection.find()
    res = open(saveLocation2,'w')
    latitude = float(myLocation[0])
    longitude = float(myLocation[1])
    temp = []

    for each in results:
        lat = each["latitude"]
        lon = each["longitude"]
        cat = each["categories"]
        dis = distance(latitude,longitude,float(lat),float(lon))
        
        if (maxDistance >= dis):
            if len(list(set(cat)&set(categoriesToSearch)))!=0:
                temp.append(each["name"])

    for each in temp:
        res.write(each)
        res.write("\n")
    
    res.close()
    


def distance(lat1,lon1,lat2,lon2):
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    
    DeltaPhi = math.radians(lat2-lat1)
    DeltaLam = math.radians(lon2-lon1)

    Ans = (math.sin(DeltaPhi/2)*math.sin(DeltaPhi/2))+(math.cos(phi1)*math.cos(phi2)*math.sin(DeltaLam/2)*math.sin(DeltaLam/2))
    c = 2*math.atan2(math.sqrt(Ans),math.sqrt(1-Ans))
    return 3959*c
