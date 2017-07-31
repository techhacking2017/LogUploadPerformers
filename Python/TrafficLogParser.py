import json
import csv
import time

from os import listdir
from os.path import isfile, join
from datetime import datetime
from time import mktime
from collections import defaultdict

iSingleNumber = 0

class tags:
    host = "server01"
    region = "ES-IS"

class fields:
    value = 0.64

with open("Dropbox.csv",newline='') as csvfile:
    dropBoxReader = csv.reader(csvfile)
    startTime = datetime.now()
    for row in dropBoxReader:
        if iSingleNumber > 0:
            templatePath = "Template\\" + row[2] + ".csv"
            trafficData = row[4]
            if trafficData == "TRUE":
                with open(templatePath, newline='') as csvfile1:
                    templateReader = csv.reader(csvfile1)
                    iSingleNumber1 = 0
                    sDelimiter = ""
                    for row1 in templateReader:
                        if iSingleNumber1 > 0:
                            if row1[1] == "Comma":
                                sDelimiter = ","
                            else:
                                sDelimiter = row1[1]
                        else:
                            iSingleNumber1 = 1
                    onlyfiles = [f for f in listdir(row[0]) if isfile(join(row[0],f))]
                    if len(onlyfiles) > 0:
                        dayIndex = int(row1[2])
                        if row1[3] != "":
                            timeIndex = int(row1[3])
                        urlIndex = int(row1[7])
                        if row1[8] != "":
                            qsIndex = int(row1[8])
                        yearStartIndex = int(row1[14])
                        yearEndIndex =int(row1[15])
                        monthStartIndex = int(row1[16])
                        monthEndIndex = int(row1[17])
                        dayStartIndex = int(row1[18])
                        dayEndIndex = int(row1[19])
                        hourStartIndex = int(row1[20])
                        hourEndIndex = int(row1[21])
                        minStartIndex = int(row1[22])
                        minEndIndex = int(row1[23])
                        lstDateValues = {}
                        iSingleIndex = 0
                        for singleFile in onlyfiles:
                            if row[1] in singleFile:
                                file = open(row[0] + singleFile,"r")
                                for line in file:
                                    if line not in row1[10]:
                                        singleLineSplit = line.split(sDelimiter)
                                        if len(singleLineSplit) > 9:
                                            if row1[3] == "":
                                                dateValue = singleLineSplit[dayIndex]
                                            else:
                                                dateValue = singleLineSplit[dayIndex] + ' ' + singleLineSplit[timeIndex]
                                            try:
                                                dateDate  = datetime(int(dateValue[yearStartIndex:yearEndIndex]),
                                                                     int(dateValue[monthStartIndex:monthEndIndex]),
                                                                     int(dateValue[dayStartIndex:dayEndIndex]),
                                                                     int(dateValue[hourStartIndex:hourEndIndex]),
                                                                     int(dateValue[minStartIndex:minEndIndex]))
                                                if iSingleIndex <= 500:
                                                    lstDateValues[iSingleIndex] = str(dateDate)
                                                    iSingleIndex = iSingleIndex + 1
                                                else:
                                                    appearances = defaultdict(int)
                                                    for curr in lstDateValues:
                                                        appearances[lstDateValues[curr]] += 1
                                                    json_string = ""
                                                    for singleKey,singleValue in appearances.items():
                                                        keyData = str(singleKey)
                                                        valueData = int(singleValue)
                                                        server = "TestServer"
                                                        application = "TestApplication"
                                                        date = {}
                                                        myTags = tags()
                                                        myFields = fields()
                                                        date['tags'] = { 'ServerName':server, 'ApplicationName':application}
                                                        date['time'] = keyData
                                                        date['fields'] = { 'value':valueData}
                                                        json_data = json.dumps(date)
                                                        if json_string == "":
                                                            json_string = "[" + json_data
                                                        else:
                                                            json_string = json_string + "," + json_data
                                                        json_string = json_string + "]"

                                                        lstDateValues = {}
                                                        iSingleIndex = 0
                                                        lstDateValues[iSingleIndex] = str(dateDate)
                                            except ValueError:
                                                print("Error Occured")
                                if iSingleIndex > 0:
                                    for curr in lstDateValues:
                                        appearances[lstDateValues[curr]] += 1
        else:
            iSingleNumber = iSingleNumber + 1

    endTime = datetime.now()
    print(startTime)
    print(endTime)


    #POC to be used in above for influx insertion



    dates = {}
    dateIndex = 0

    date = {}
    myTags = tags()
    myFields = fields()

    date['tags'] = { 'host':myTags.host, 'region':myTags.region}
    date['time'] = '2009-11-10T24:00:00Z'
    date['fields'] = { 'value':myFields.value}

    dates[dateIndex] = date

    json_data = json.dumps(dates)
    print(json_data)