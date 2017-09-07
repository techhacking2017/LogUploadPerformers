import json
import csv
import time
import os
import shutil

from os import listdir
from os.path import isfile, join
from datetime import datetime
from time import mktime
from collections import defaultdict
from influxdb import InfluxDBClient
from pytz import timezone
from pprint import pprint


iSingleNumber = 0

class tags:
    host = "server01"
    region = "ES-IS"

class fields:
    value = 0.64

with open('config.json') as data_file:
    data = json.load(data_file)

influxServer = data["InfluxServer"]
portNo = data["PortNo"]
userName = data["UserName"]
passwordValue = data["Password"]
databaseName = data["DatabaseName"]

localFilePath = os.path.dirname(os.path.abspath(__file__))
with open(localFilePath + "\\Dropbox.csv",newline='') as csvfile:
    dropBoxReader = csv.reader(csvfile)
    startTime = datetime.now()
    for row in dropBoxReader:
        if iSingleNumber > 0:
            appName = row[7]
            templateName = row[2]
            templatePath = localFilePath + "Template\\" + row[2] + ".csv"
            destinationPath = localFilePath + '\\' + templateName + '\\'
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
                            iSingleNumber1 = iSingleNumber1 + 1
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
                        serverIndex = str(row1[9])
                        if serverIndex != "-1":
                            serverIndex = int(row1[9])
                        timeZoneValue = str(row1[6])
                        dateTimeFormat = str(row1[5])
                        monthInfos = str(row1[24])
                        singleDayMonth = str(row1[25])

                        if monthInfos != "null":
                            monthDetails = monthInfos.split("!")
                        lstDateValues = {}
                        lstServerNames = {}
                        iSingleIndex = 0
                        iterationNumber = 1
                        for singleFile in onlyfiles:
                            if row[1] in singleFile:
                                destinationFile = destinationPath + singleFile
                                if not os.path.exists(destinationPath):
                                    os.makedirs(destinationPath)
                                shutil.copy2(row[0] + singleFile, destinationFile)
                                file = open(row[0] + singleFile,"r")
                                for line in file:
                                    if line not in row1[10]:
                                        singleLineSplit = line.split(sDelimiter)
                                        if len(singleLineSplit) > 9:
                                            if row1[3] == "":
                                                dateValue = singleLineSplit[dayIndex]
                                            else:
                                                dateValue = singleLineSplit[dayIndex] + ' ' + singleLineSplit[timeIndex]
                                            serverName = ""
                                            if serverIndex == "-1":
                                                serverName = "TestServer"
                                            else:
                                                serverName = singleLineSplit[serverIndex]
                                                serverName = serverName.replace('"','')
                                            try:
                                                monthData = dateValue[monthStartIndex:monthEndIndex+1]
                                                if singleDayMonth == "no":
                                                    if monthInfos == "null":
                                                        dateDate = datetime(int(dateValue[yearStartIndex:yearEndIndex+1]),
                                                                            int(monthData),
                                                                            int(dateValue[dayStartIndex:dayEndIndex+1]),
                                                                            int(dateValue[hourStartIndex:hourEndIndex+1]),
                                                                            int(dateValue[minStartIndex:minEndIndex+1]),0,0)
                                                    else:
                                                        monthIndex = monthDetails.index(monthData) + 1
                                                        dateDate = datetime(int(dateValue[yearStartIndex:yearEndIndex+1]),
                                                                            monthIndex,
                                                                            int(dateValue[dayStartIndex:dayEndIndex+1]),
                                                                            int(dateValue[hourStartIndex:hourEndIndex+1]),
                                                                            int(dateValue[minStartIndex:minEndIndex+1]),0,0)
                                                else:
                                                    dateDate = datetime.strptime(dateValue, dateTimeFormat)
                                                dateDateUTC = timezone(timeZoneValue).localize(dateDate).astimezone(timezone('UTC'))
                                                if iSingleIndex <= 3500:
                                                    lstDateValues[iSingleIndex] = str(dateDate)
                                                    lstServerNames[iSingleIndex] = str(serverName)
                                                    iSingleIndex = iSingleIndex + 1
                                                else:
                                                    appearances = defaultdict(int)
                                                    for curr in lstDateValues:
                                                        appearances[lstServerNames[curr] + "!@!" + lstDateValues[curr]] += 1
                                                    json_string = ""
                                                    for singleKey,singleValue in appearances.items():
                                                        keyData = str(singleKey)
                                                        keyDatas = keyData.split("!@!")
                                                        valueData = int(singleValue)
                                                        server = keyDatas[0]
                                                        application = appName
                                                        date = {}
                                                        myTags = tags()
                                                        myFields = fields()
                                                        date['measurement'] = "tblAccessLogROneMin"
                                                        date['tags'] = { 'ServerName':server, 'ApplicationName':application, 'IterationTag':str(iterationNumber), 'FilePathCheck':destinationFile,'TemplateName':templateName}
                                                        date['time'] = keyDatas[1]
                                                        date['fields'] = { 'numberOfOccurences':valueData}
                                                        json_data = json.dumps(date)
                                                        if json_string == "":
                                                            json_string = "[" + json_data
                                                        else:
                                                            json_string = json_string + "," + json_data
                                                        json_string = json_string + "]"
                                                        json_object = json.loads(json_string)
                                                        client = InfluxDBClient(influxServer, portNo, userName, passwordValue, databaseName)
                                                        client.write_points(json_object)
                                                        iterationNumber = iterationNumber + 1

                                                        lstDateValues = {}
                                                        iSingleIndex = 0
                                                        lstDateValues[iSingleIndex] = str(dateDate)
                                            except ValueError:
                                                print("Error Occured")
                                file.close()
                                if iSingleIndex > 0:
                                    for curr in lstDateValues:
                                        appearances[lstDateValues[curr]] += 1
                        for singleFile in onlyfiles:
                            if row[1] in singleFile:
                                os.remove(row[0] + singleFile)
        else:
            iSingleNumber = iSingleNumber + 1

    endTime = datetime.now()
    print(startTime)
    print(endTime)