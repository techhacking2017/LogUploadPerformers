library(stringr)
library(lubridate)
library(xts)
library(highfrequency)
library(influxdbr2)
library(jsonlite)

configData <- fromJSON("configFile.json")

con <- influxdbr2::influx_connection(host = configData$HostName[1],
                                     port = configData$PortNo[1],
                                     user = configData$UserName[1],
                                     pass = configData$Password[1])

GetDropboxDetails = function() {
  if(file.size("Dropbox.csv") == 0) {
    dropboxDetails <- data.frame()
    stop("Dropbox should have file mapping details in it")
  }
  else{
    dropboxDetails <- read.csv("Dropbox.csv")
  }
  return(dropboxDetails)
}

DoubleQuoteRemove = function(dateValue) {
  returnValue <- gsub('"','',dateValue)
  return(returnValue)
}

DateTimeMiddleSeperatorClenser <- function(dateValue, dateTimeMiddleSeperator) {
  returnData <- dateValue
  if(is.na(dateTimeMiddleSeperator) == FALSE && is.null(dateTimeMiddleSeperator) == FALSE && length(dateTimeMiddleSeperator) > 0) {
    sReplace <- str_sub(dateValue, str_locate(dateValue, dateTimeMiddleSeperator)[1], str_locate(dateValue, dateTimeMiddleSeperator)[2])
    returnData <- str_replace(dateValue, sReplace, str_replace(sReplace, str_sub(sReplace,str_length(sReplace))," "))
  }
  return(returnData)
}

ValidateFile <- function(singleFilePath, wildCard) {
  existCount <- str_count(singleFilePath, wildCard)
  return(existCount > 0)
}

GetTemplateDetails <- function(templateName) {
  templateDetail <- data.frame()
  templateDetail <- read.csv(paste0("Template\\",templateName))
  return(templateDetail)
}

IsDate <- function(myDate, data.format = "%d/%m/%y") {
  tryCatch(!is.na(strptime(myDate,toString(data.format))), error = function(err) {FALSE})
}

InsertToInflux <- function(xts_Data,measurmentName) {
  influxdbr2::influx_write(
    con = con,
    db = configData$dbName,
    xts = xts_Data,
    measurement = measurmentName,
    precision = "s"
  )
}

InsertToInfluxAccess <- function(xtsObject, iterationNumber, serverName, filePathToStore, templateName) {
  xtsRows <- nrow(xtsObject)
  xtsFrameVal <- NULL
  for(iIndex in 1:xtsRows) {
    testData <- as.numeric(index(xtsObject[iIndex,1]))
    dataFrameVal <- data.frame(dateValue=numeric(1),numberofOccurences=numeric(1),Iterations=numeric(1), ServerName = character(1), seqNumber = character(1), stringsAsFactors = FALSE)
    dataFrameVal$dateValue[1] = testData
    dataFrameVal$numberofOccurences[1] = as.numeric(xtsObject[iIndex,1])
    dataFrameVal$Iterations[1] = iterationNumber
    dataFrameVal$ServerName[1] = serverName
    dataFrameVal$seqNumber[1] = paste0("seq",as.character(iterationNumber))
    if(is.null(xtsFrameVal) == TRUE) {
      xtsFrameVal <- as.xts(dataFrameVal[,2:5],order.by = as.POSIXct(dataFrameVal[,1],origin = "1970-01-01 00:00:00"))
    }
    else {
      xtsFrameVal <- rbind(xtsFrameVal,as.xts(dataFrameVal[,2:5],order.by = as.POSIXct(dataFrameVal[,1],origin = "1970-01-01 00:00:00")))
    }
  }
  lst_tag_Attributes <- list(FilePathCheck = filePathToStore, TemplateName = templateName)
  xts::xtsAttributes(xtsFrameVal) <- lst_tag_Attributes
  InsertToInflux(xtsFrameVal,"tblAccessLogOneMin")
}

AdjustDate <- function(singleDate) {
  numericTime <- as.numeric(singleDate)
  numericTimeUpdated <- numericTime - (numericTime%%60)
  dtStartUpdated <- as.POSIXct(numericTimeUpdated,origin = "1970-01-01 00:00:00", tz = "UTC")
  return(dtStartUpdated)
}

ExecuteFileData <- function(templateName, filePath, serverName, applicationName, filePathToStore) {
  templateDetails <- GetTemplateDetails(templateName)
  
  serverIndex <- templateDetails[1,10]
  dateTimeMiddleSeperator <- templateDetails[1,5]
  dateTimeFormat <- templateDetails[1,6]
  dateTimeZone <- templateDetails[1,7]
  
  filecon <- file(filePath,"r")
  
  iIterationIndex <- 0
  iLinesConsidered <- 0
  
  while(TRUE) {
    is_ValidURL <- FALSE
    lineChunks <- readLines(filecon, n = 1500)
    if(length(lineChunks) == 0) {
      print("completed")
      break
    }
    
    dateValues <- numeric()
    serverNames <- character()
    iIndexValue <- 1
    for(iSubFileIndex in 1:length(lineChunks)) {
      singleLine <- lineChunks[iSubFileIndex]
      delimiterName <- ""
      if(templateDetails[1,2] == "Comma") {
        delimiterName = ","
      }
      else if(templateDetails[1,2] == "Space") {
        delimiterName = " "
      }
      else {
        delimiterName = templateDetails[1,2]
      }
      splittedLines <- strsplit(singleLine,delimiterName)
      dateValue <- ""
      if(is.na(templateDetails[1,4]) == FALSE) {
        dateValue <- paste0(splittedLines[[1]][templateDetails[1,3]]," ",splittedLines[[1]][templateDetails[1,4]])
      }
      else {
        dateValue <- splittedLines[[1]][templateDetails[1,3]]
      }
      dateValue <- DoubleQuoteRemove(dateValue)
      dateTimeMiddleSeperator = templateDetails[1,5]
      dateTimeFormat = templateDetails[1,6]
      dateTimeZone = templateDetails[1,7]
      dateValue = DateTimeMiddleSeperatorClenser(dateValue,dateTimeMiddleSeperator)
      if(IsDate(dateValue,dateTimeFormat) == TRUE) {
        singleDateTime <- strptime(dateValue,toString(dateTimeFormat), tz = toString(dateTimeZone))
        singleDateTimeUpdated <- as.POSIXct(singleDateTime)
        singleDateTimeUpdated1 <- as.POSIXlt(singleDateTimeUpdated,"UTC")
        singleDateTimeUpdated1 <- AdjustDate(singleDateTimeUpdated1)
        dateValues[iIndexValue] <- as.numeric(singleDateTimeUpdated1)
        
        if(serverIndex == -1) {
          serverNames[iIndexValue] <- DoubleQuoteRemove(serverName)
        }
        else {
          serverName = splittedLines[[1]][serverIndex]
          serverNames[iIndexValue] <- DoubleQuoteRemove(serverName)
        }
        iIndexValue <- iIndexValue + 1
      }
      else {
      }
      totLength <- length(dateValues)
      iIterationIndex <- iIterationIndex + 1
      dFrame <- data.frame(DateTimeStamp = numeric(totLength), 
                           NumberOfOccurences = numeric(totLength), 
                           ServerValue = character(totLength), stringsAsFactors = FALSE)
      for(iCheckIndex in 1:totLength) {
        dFrame$DateTimeStamp[iCheckIndex] <- dateValues[iCheckIndex]
        dFrame$NumberOfOccurences[iCheckIndex] <- 1
        dFrame$SeeerverValue[iCheckIndex] <- serverNames[iCheckIndex]
      }
      dFrameSegregation <- split(dFrame,as.factor(dFrame$ServerValue))
      lapply(dFrameSegregation, function(x) {
        serverNameValue <-  toString(x[1,3])
        singleDFrame <- x
        xtsFrame <- as.xts(singleDFrame[,2],order.by = as.POSIXct(singleDFrame[,1],origin = "1970-01-01 00:00:00"))
        ends <- endpoints(xtsFrame,'minutes',1)
        xtsFrameAggregated <- period.apply(xtsFrame,ends,sum)
        InsertToInfluxAccess(xtsFrameAggregated,iIterationIndex,serverNameValue,filePathToStore,templateName)
      })
    }
    close(filecon)
  }
}

MoveFilePath = function(filePath,templateName,fileName) {
  dir.create("ImportData", recursive = T)
  dir.create(paste0("ImportData/",templateName), recursive = T)
  file.copy(from = filePath, to=paste0("ImportData/",templateName,"/"), recursive = TRUE, copy.mode = TRUE)
  return(paste0("ImportData/",templateName,"/",fileName))
}

GetDropBoxLine = function(singleDropBox) {
  if(length(singleDropBox) == 0) {
    filePath <- toString(singleDropBox[1])
    wildCard <- toString(singleDropBox[2])
    templateName <- toString(singleDropBox[3])
    serverName <- toString(singleDropBox[4])
    acmFlag <- as.logical(str_trim(toString(singleDropBox[5])))
    dbName <- toString(singleDropBox[6])
    applicationName <- toString(singleDropBox[8])
    
    allFiles <- dir(filePath)
    if(length(allFiles) > 0) {
      for(iIndex in 1:length(allFiles)) {
        filePathToUse <- paste0(filePath,toString(allFiles[iIndex]))
        if(ValidateFile(filePathToUse,wildCard) == TRUE) {
          if(acmFlag == TRUE) {
            filePathToStore <- MoveFilePath(filePathToUse, templateName, allFiles[iIndex])
            ExecuteFileData(templateName,filePathToUse,serverName,applicationName,filePathToStore)
            if(file.exists(filePathToUse)) {
              file.remove(filePathToUse)
            }
          }
        }
      }
    }
  }
}

ExecuteFile = function(dropBoxDetails) {
  if(nrow(dropBoxDetails) > 0) {
    apply(dropBoxDetails,1,GetDropBoxLine)
  }
}

dropBoxDetails <- GetDropboxDetails()
if(nrow(dropBoxDetails) > 0) {
  startTime <- paste0("Start Time ",Sys.time())
  ExecuteFile(dropBoxDetails)
  print(startTime)
  print(paste0("End Time ",Sys.time()))
}