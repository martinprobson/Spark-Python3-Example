'''
Created on 22 Nov 2017

@author: martinr
'''

import logging
from utils import setupLogging,versionInfo,getAllConf,getInputData,sparkEnv


setupLogging()
sparkEnv()
from pyspark.sql import SparkSession  # @UnresolvedImport
from pyspark.sql.types import StructType,StructField,LongType,DateType,StringType,Row  # @UnresolvedImport


spark = SparkSession.builder.appName("SparkTest").master("local[50]").getOrCreate()


logger = logging.getLogger(__name__)
logger.info("Starting....")
[logger.info(s) for s in versionInfo(spark).split('\n')]
[logger.info(s) for s in getAllConf(spark).split('\n')]

empSchema = StructType([ StructField("emp_no",LongType(),False),
                        StructField("birth_date",DateType(),False),
                        StructField("first_name",StringType(),True),
                        StructField("last_name",StringType(),True),
                        StructField("gender",StringType(),True),
                        StructField("hire_date",DateType(),False) ])

titlesSchema = StructType([ StructField("emp_no",LongType(),False),
                            StructField("title",StringType(),False),
                            StructField("from_date",DateType(),True),
                            StructField("to_date",DateType(),True) ])

empsRDD = spark.sparkContext.parallelize(getInputData("data/employees.json"))
empsDF  = spark.read.schema(empSchema).json(empsRDD)
empsDF.createOrReplaceTempView("employees")

titlesRDD = spark.sparkContext.parallelize(getInputData("data/titles.json"))
titlesDF  = spark.read.schema(titlesSchema).json(titlesRDD)
titlesDF.createOrReplaceTempView("titles")

title = spark.sql("select * from titles") \
             .select('emp_no','title','from_date','to_date') \
             .where("from_date <= current_date and to_date > current_date")
title.cache()        
        
emp = spark.table("employees")
emp.cache()
result = emp.alias('employee') \
            .select('emp_no','first_name','last_name') \
            .join(title.alias('title'),emp['emp_no'] == title['emp_no']) \
            .select("employee.emp_no","employee.first_name","employee.last_name","title.title")
r = result.collect()            
for row in result.collect():
    print(row)
logger.info("Stopped....")  

