import json
import csv
import time
import os

from os import listdir
from os.path import isfile, join
from datetime import datetime
from influxdb import InfluxDBClient
from pytz import timezone

class tags:
    host = "server-01"
    region = "us-west"

class fields:
    value = 0.64

class WorkLoad:
    workLoadName = ""
    StartHour = 0
    StartMin = 0
    EndHour = 0
    EndMin = 0

with open('config.json') as data_file:
    data = json.load(data_file)

influxServer = data["InfluxServer"]
portNo = data["PortNo"]
userName = data["UserName"]
passwordValue = data["Password"]
databaseName = data["DatabaseName"]

localFilePath = os.path.dirname(os.path.abspath(__file__))
iSingleNumber = 0

workLoadList = []
with open(localFilePath + '\\workloads.csv', newline='') as workloadfile:
    iWorkLoadNumber = 0
    workLoadReader = csv.reader(workloadfile)
    for workLoadRow in workLoadReader:
        if iWorkLoadNumber > 0:
            singleWorkLoad = WorkLoad()
            singleWorkLoad.workLoadName = str(workLoadRow[0])
            singleWorkLoad.StartHour = int(workLoadRow[1])
            singleWorkLoad.StartMin = int(workLoadRow[2])
            singleWorkLoad.EndHour = int(workLoadRow[3])
            singleWorkLoad.EndMin = int(workLoadRow[4])
            workLoadList.append(singleWorkLoad)
        else:
            iWorkLoadNumber = 1

with open(localFilePath + '\\Dropbox.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    startTime = datetime.now()
    for row in spamreader:
        if iSingleNumber > 0:
            templatePath = localFilePath + '\\Template\\' + row[2] + '.csv'
            trafficData = row[4]
            if trafficData == "FALSE":
                with open(templatePath, newline='') as csvfile1:
                    spamreader1 = csv.reader(csvfile1)
                    iSingleNumber1 = 0
                    sDelimiter = ""
                    for row1 in spamreader1:
                        if iSingleNumber1 > 0:
                            if row1[1] == "Comma":
                                sDelimiter = ","
                            else:
                                sDelimiter = row1[1]
                        else:
                            iSingleNumber1 = 1
                    onlyfiles = [f for f in listdir(row[0]) if isfile(join(row[0], f))]
                    if(len(onlyfiles) > 0):
                        dayIndex = int(row1[2])
                        if row1[3] != "":
                            timeIndex = int(row1[3])
                        dateTimeFormat = str(row1[5])
                        valueIndex = int(row1[8])
                        kcdValue = str(row1[0])
                        keyIndex = ""
                        if row1[7] != "":
                            keyIndex = int(row1[7])
                        resourceType = str(row1[0])
                        serverIndex = int(row1[9])
                        yearStartIndex = int(row1[14])
                        yearEndIndex = int(row1[15])
                        MonthStartIndex = int(row1[16])
                        MonthEndIndex = int(row1[17])
                        DayStartIndex = int(row1[18])
                        DayEndIndex = int(row1[19])
                        HourStartIndex = int(row1[20])
                        HourEndIndex = int(row1[21])
                        MinStartIndex = int(row1[22])
                        MinEndIndex = int(row1[23])
                        monthInfos = str(row1[24])
                        singleDayMonth = str(row1[25])

                        if monthInfos != "null":
                            monthDetails = monthInfos.split("|")
                        timeZoneValue = str(row1[6])
                        iIterationNumber = 1
                        for singleFile in onlyfiles:
                            if row[1] in singleFile:
                                file = open(row[0] + singleFile, "r")
                                json_string = ""
                                applicationName = str(row[7])
                                for line in file:
                                    singleLineSplits = line.split(sDelimiter)
                                    if len(singleLineSplits) > 3:
                                        if row1[3] == "":
                                            dateValue = singleLineSplits[dayIndex]
                                        else:
                                            dateValue = singleLineSplits[dayIndex] + ' ' + singleLineSplits[timeIndex]
                                        resourceValue = singleLineSplits[valueIndex]
                                        resourceValue = resourceValue.replace('"','')
                                        if keyIndex == "":
                                            keyData = "NoKey"
                                        else:
                                            keyData = singleLineSplits[keyIndex]
                                            keyData = keyData.replace('"','')
                                        try:
                                            monthData = dateValue[MonthStartIndex:MonthEndIndex + 1]
                                            if singleDayMonth == "no":
                                                if monthInfos == "null":
                                                    dateDate = datetime(int(dateValue[yearStartIndex:yearEndIndex + 1]),
                                                                        int(monthData),
                                                                        int(dateValue[DayStartIndex:DayEndIndex+1]),
                                                                        int(dateValue[HourStartIndex:HourEndIndex+1]),
                                                                        int(dateValue[MinStartIndex:MinEndIndex+1]),0,0)
                                                else:
                                                    monthIndex = monthDetails.index(monthData) + 1
                                                    dateDate = datetime(int(dateValue[yearStartIndex:yearEndIndex + 1]),
                                                                        monthIndex,
                                                                        int(dateValue[DayStartIndex:DayEndIndex+1]),
                                                                        int(dateValue[HourStartIndex:HourEndIndex+1]),
                                                                        int(dateValue[MinStartIndex:MinEndIndex+1]),0,0)
                                            else:
                                                dateDate = datetime.strptime(dateValue,dateTimeFormat)
                                            dateDateUTC = timezone(timeZoneValue).localize(dateDate).astimezone(timezone('UTC'))
                                            yearValue = dateDateUTC.year
                                            monthValue = dateDateUTC.month
                                            dayValue = dateDateUTC.day
                                            workLoadValue = "NoWorkLoad"
                                            for singleWorkLoadData in workLoadList:
                                                wlName = singleWorkLoadData.workLoadName
                                                startHour = singleWorkLoadData.StartHour
                                                startMin = singleWorkLoadData.StartMin
                                                endHour = singleWorkLoadData.EndHour
                                                endMin = singleWorkLoadData.EndMin
                                                startDateValue = datetime(yearValue, monthValue,dayValue,startHour,startMin,0,0,timezone('UTC'))
                                                endDateValue = datetime(yearValue, monthValue,dayValue,endHour,endMin,0,0,timezone('UTC'))
                                                if startDateValue <= dateDateUTC and endDateValue >= dateDateUTC:
                                                    workLoadValue = wlName
                                                if resourceValue != "":
                                                    resourceValueData = int(round(float(resourceValue),0))
                                                else:
                                                    resourceValueData = 0
                                                serverName = ""
                                                if serverIndex != -1:
                                                    serverName = singleLineSplits[serverIndex]
                                                else:
                                                    serverName = str(row[3])
                                                date = {}
                                                myTags = tags()
                                                myFields = fields()
                                                date['measurement'] = "tbl_resource_log"
                                                date['tags'] = { 'ServerName':serverName, 'KCDData':kcdValue, 'KeyData':keyData, 'WorkLoadData':workLoadValue}
                                                data['time'] = str(dateDateUTC)
                                                data['fields'] = {'ResourceValue':resourceValueData, 'FileData':str(dateDate)}
                                                json_data = json.dumps(date)
                                                if json_string == "":
                                                    json_string = "[" + json_data
                                                else:
                                                    json_string = json_string + "," + json_data
                                            if iIterationNumber == 3500:
                                                iIterationNumber = 1
                                                json_string = json_string + "]"
                                                json_object = json.loads(json_string)
                                                client = InfluxDBClient(influxServer,portNo, userName, passwordValue, databaseName)
                                                client.write_points(json_object)
                                                json_string = ""
                                            else:
                                                iIterationNumber = iIterationNumber + 1
                                        except ValueError:
                                            print("Error Occued")
                                if json_string != "":
                                    iIterationNumber = 1
                                    json_string = json_string + "]"
                                    json_object = json.loads(json_string)
                                    client = InfluxDBClient(influxServer,portNo,userName,passwordValue,databaseName)
                                    client.write_points(json_object)
                                    json_string = ""
                        file.close()
                        for singleFile in onlyfiles:
                            if row[1] in singleFile:
                                os.remove(row[0] + singleFile)
                        if iIterationNumber > 1:
                            iIterationNumber = 1
                            json_string = json_string + "]"
                            json_object = json.loads(json_string)
                            client = InfluxDBClient(influxServer, portNo, userName, passwordValue, databaseName)
                            client.write_points(json_object)
                            json_string = ""
        else:
            iSingleNumber = iSingleNumber + 1
    endTime = datetime.now()
    print(startTime)
    print(endTime)