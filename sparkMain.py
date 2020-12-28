import pyspark.sql
from pyspark.sql import SparkSession

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import pykafka 
import os
import configparser


def myDriverFunction():
    spark = SparkSession.builder.master('local').appName('kafkaAssignment').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    readSpreadSheet(spark)


def readIndividualSheet(index,spreadSheet,spark):
    sheet = spreadSheet.get_worksheet(index)
    eachrow = sheet.get_all_values()
    sheetDF = spark.createDataFrame(eachrow)
    #print('This is from sheet {}'.format(str(index+1)))
    sheetDF.show()
    return eachrow

def readSpreadSheet(spark):
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)


    spreadSheet = client.open("ReadMe")                                         # Name of google spreadsheet

    for i in range(3):
        sheetData=readIndividualSheet(i,spreadSheet,spark)
        createKafkaTopic(i+1,sheetData)


def createKafkaTopic(sheetID,sheetData):
    #Project_loc= os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    #Config = configparser.ConfigParser()
    #Config.read(Project_loc + '/Configs/kafka_config.ini')
    
    client= pykafka.KafkaClient('localhost:9092')                               # Kafka runs at default port of 9092

    topic = client.topics['TopicForSheetno_'+str(sheetID)]                      # This will create the topic if they does not exist
    producer = topic.get_producer()
    for listA in sheetData:
        for data in listA:
            producer.produce(bytes(str(data),"utf-8"))                          # it produces the data of sheets to their respective topics
  

if __name__ == '__main__':
    myDriverFunction()