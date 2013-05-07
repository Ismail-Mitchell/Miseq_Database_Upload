#!/usr/bin/python
#Modules to Import:
from xml.dom.minidom import parseString
import re
import csv

def readxml (readfile):
    "This is the function that returns the data from a xml file"
    #open the xml file for reading:
    file = open(readfile,'r');
    #convert to string:
    data = file.read();
    #close file because we dont need it anymore:
    file.close();
    return data;


def rParameters (data1, tag1): 
    "This is a function that returns the data from a xml tag that you specify"
    #parse the xml file
    dom = parseString(data1);
    #retrieve the first xml tag (<tag>data</tag>) that the parser finds with name tagName:
    xmlTag = dom.getElementsByTagName(tag1)[0].toxml();
    #strip off the tag (<tag>data</tag>  --->   data):
    xmlData1=xmlTag.replace('<'+tag1+'>','').replace('</'+tag1+'>','');
    return xmlData1;


def rParameters2 (data1, tag1, tag2):
    "This is a function that returns the data from 2 xml tag that you specify"
    #parse the xml file
    dom = parseString(data1);
    #get the second level of a xml tree nodes
    xmlData2 = dom.getElementsByTagName(tag1)[0].getElementsByTagName(tag2)[0].firstChild.data
    return xmlData2;



#Creating str data for xml file
RunFile = readxml('RunParameters.xml')

#Creating Dictionary for values we want to upload to mysql
RunValues = {'PR2BottleRFIDTag-SerialNumber': 'x',
             'PR2BottleRFIDTag-ExpirationDate': 'x',
             'FlowcellRFIDTag-SerialNumber': 'x',
             'FlowcellRFIDTag-ExpirationDate': 'x',
             'ReagentKitRFIDTag-SerialNumber': 'x',
             'ReagentKitRFIDTag-ExpirationDate': 'x',
             'Setup-ApplicationVersion': 'x',
             'Setup-ApplicationName': 'x',
             'Workflow-Analysis': 'x',
             'RunID': 'x',
             'ScannerID': 'x',
             'ExperimentName': 'x',
             'RTAVersion': 'x',
             'SampleSheetName': 'x',
             'OutputFolder': 'x',
             'AnalysisFolder': 'x',
             'FPGAVersion': 'x',
             'MCSVersion': 'x',
             'RunStartDate': 'x',
             'Reads': 'x',
             }

#iterating through keys in the dictionary and feeding them into functions above to retrive xml tag values
for key in RunValues:
    if '-' in key:
       splittag = re.split('-', key)
       result1 = rParameters2(RunFile, splittag[0], splittag[1])
       RunValues[key] = result1
    elif '-' not in key:
       result2 = rParameters(RunFile,key)
       RunValues[key] = result2
    else:
       print ('Nothing Happened')




#Trimming off TIME STAMP for the date values below
RunValues['PR2BottleRFIDTag-ExpirationDate'] = RunValues['PR2BottleRFIDTag-ExpirationDate'][:-9]
RunValues['FlowcellRFIDTag-ExpirationDate'] = RunValues['FlowcellRFIDTag-ExpirationDate'][:-9]
RunValues['ReagentKitRFIDTag-ExpirationDate'] = RunValues['ReagentKitRFIDTag-ExpirationDate'][:-9]

#Printing Values to the Screen
#for item in RunValues:
#    print (item + '.............' + RunValues[item])






#--------------------------------------------END OF RunParameters XML PARSING-----------------------------------

#---------------------------------Extracting Information from SampleSheet.csv files-----------------------------------------


#This script it sued to parse through an illumina sample sheet and return information in a dictionary format
reader = csv.reader(open('SampleSheet.csv', "r"), delimiter = ",", skipinitialspace=True)

values = []
#creating general dictionary for sample spreadsheet
sampleinfo = {'Investigator': 'x',
              'Project Name': 'x',
              'Experiment Name': 'x',
              'Workflow': 'x',
              'Assay': 'x',
              'Description': 'x',
              'Date': 'x',
              'Application': 'x'
              }

#Go through each line in the csv file and parse out information
for line in reader:
    if 'Investigator' in line[0]:
        sampleinfo['Investigator'] = line[1];
    elif 'Project Name' in line[0]:
        sampleinfo['Project Name'] = line[1];
    elif 'Experiment Name' in line[0]:
        sampleinfo['Experiment Name'] = line[1];
    elif 'Workflow' in line[0]:
        sampleinfo['Workflow'] = line[1];
    elif 'Assay' in line[0]:
        sampleinfo['Assay'] = line[1];
    elif 'Description' in line[0]:
        sampleinfo['Description'] = line[1];
    elif 'Date' in line[0]:
        sampleinfo['Date'] = line[1];
    elif 'Application' in line[0]:
        sampleinfo['Application'] = line[1];
    else:
      values.append(line)

#split up each entry in the values list to one entry per list[] in values2
values2 = []
for row in values:
  for entry in range(0,8): #range is the number of columns in the csv file
    values2.append(row[entry])

#set the start reading place for the samples
y = 120


listofsamples = []
      
while(y < len(values2)):
            sampleval = {'Sample_ID': values2[y],
            'Sample_Name': values2[y+1],
            'I7_Index_ID': values2[y+4],
            'index': values2[y+5],
            'Sample_Project': values2[y+6]
            }
            y += 8
            listofsamples.append(sampleval)

#Each sample would get uploaded into the MySQL data with the corresponding information
#for listnum in range(0,len(listofsamples)):
#    print (listofsamples[listnum]['Sample_ID'])
  
#-----------------------------------END OF SAMPLE SHEET CSV DATA EXTRACTION--------------------------

#-----------------------------------UPLOADING TO MYSQL DATABSE - AUTOMATION-------------------------- 

import MySQLdb

db = MySQLdb.connect(host="host", # host
                     user="user", # username
                      passwd="pass", # password
                      db="db") # datbase

# Creating a Cursor Object that executes the queries
cur = db.cursor() 

# Upolading DATA from xml sheet and csv sheet into MySQL database

cur.execute("INSERT INTO RunInfo (DATE, FlowCellBarcode, FlowCellExpiry, PR2BottleBarcode, PR2BottleExpiry, ReagentKitBarcode, ReagentKitExpiry, InstrumentScannerID, ControlSoftwareVersion, RealTimeAnalysisVersion, ExperimentName, WorkflowAnalysis, RunInfoRead1, RunInfoRead2, RunInfoRead3) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '151', '6', '151');", (sampleinfo['Date'], RunValues['FlowcellRFIDTag-SerialNumber'], RunValues['FlowcellRFIDTag-ExpirationDate'], RunValues['PR2BottleRFIDTag-SerialNumber'], RunValues['PR2BottleRFIDTag-ExpirationDate'], RunValues['ReagentKitRFIDTag-SerialNumber'], RunValues['ReagentKitRFIDTag-ExpirationDate'], RunValues['ScannerID'], RunValues['Setup-ApplicationVersion'], RunValues['RTAVersion'], sampleinfo['Experiment Name'], RunValues['Workflow-Analysis']))

for listnum in range(0,len(listofsamples)):
	cur.execute("INSERT INTO SampleInfo (FlowCellBarcode, Investigator, Workflow, Assay, Description, DATE, Application, ExperimentName, AccessionNumber, SampleName, IndexName, IndexSequence, SampleProject ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",(RunValues['FlowcellRFIDTag-SerialNumber'], sampleinfo['Investigator'], sampleinfo['Workflow'], sampleinfo['Assay'], sampleinfo['Description'], sampleinfo['Date'], sampleinfo['Application'], sampleinfo['Experiment Name'],listofsamples[listnum]['Sample_ID'], listofsamples[listnum]['Sample_Name'], listofsamples[listnum]['I7_Index_ID'], listofsamples[listnum]['index'], listofsamples[listnum]['Sample_Project']))


#Checking to see if everything was uploaded
cur.execute("SELECT * FROM SampleInfo")


 #print all the first cell of all the rows
for row in cur.fetchall() :
    print row[0]
  
