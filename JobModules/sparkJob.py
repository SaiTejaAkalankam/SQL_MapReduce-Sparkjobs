import findspark as discoverSpark
discoverSpark.init()
import time as Time
import json as Json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
zero = 0
one = 1
Yes = True
Nothing = None
equalTo = '='
twoEquals = '=='


''' this module
        executes the Spark job 
            on the input query and 
                sends the output of the Spark job as well as 
                    the time taken to complete it.

    A spark session is created and the web UI can be accessed at http://127.0.0.1:5000/provideQuery?query=

    The user can put the sql query after the '=' in the URL

    The Spark job then runs to find result after performing the query
'''

class sparkJob:
    def __init__(self,configurations):
        self.sparkOutput = Nothing
        self.fieldDatatype = {
            'FloatType': FloatType(),
            'IntegerType': IntegerType(),
            'StringType': StringType()
        } 
        self.keywords = ['where', 'groupby', 'agg', 'where']
        self.outputTime = Nothing
        self.configurations = configurations


    '''

            After importing the dataset

                -the processedQuery which contains the translation of sql query into the Spark Job
                -according to the actions required like
                            -SELECT
                            -GROUPBY
                            -HAVING
                            -AGGREGATE FUNCTIONS
                    the choice of statements to be executed on the dataset is chosen

    '''

    def performJob(self, processedQuery, schemaStructure, tables):
        
        #record the time when the job starts
        startTime = Time.time()

        #import the dataset to be operated upon
        table = self.importDataset(schemaStructure, tables)


        '''------WHERE ACTION LOGIC STARTS--------------------'''

        if processedQuery['whereSymbol'] == equalTo:
            whereSymbol = twoEquals
        else:
            whereSymbol = processedQuery['whereSymbol']
        if schemaStructure[tables][processedQuery['whereAttribute']] != 'StringType':
            whereClause = '{whereAttribute}{whereSymbol}{whereInput}'.format(
                whereAttribute=processedQuery['whereAttribute'],
                whereInput=processedQuery['whereInput'],
                whereSymbol=whereSymbol
            )
        else:
            whereClause = '{whereAttribute}{whereSymbol}"{whereInput}"'.format(
                whereAttribute=processedQuery['whereAttribute'],
                whereInput=processedQuery['whereInput'],
                whereSymbol=whereSymbol
            )

        whereOutput = table.where(whereClause)

        '''------WHERE ACTION LOGIC ENDS--------------------'''


        #--------------------------------------------------------------------------------


        '''------GROUPBY HAVING ACTION LOGIC STARTS--------------------'''


        groupByOutput = whereOutput.groupBy(processedQuery['groupByAttributes'])
        procQueryList = processedQuery['selectMethod']

        '''------If the query contains any AGGREGATE FNCTIONS THEN THIS LOGIC IS APPLIED--------------------'''        
        firstStr = str(procQueryList[zero][one])
        secondStr = str(procQueryList[zero][zero])
        aggOutput = groupByOutput.agg({firstStr: secondStr})

        #--------------------------------------------------------------

        havingClause = '{selectMethod}({attribute}){havingSymbol}{havingBoundaryValue}'.format(
            selectMethod=secondStr,
            attribute=firstStr,
            havingSymbol=processedQuery['havingClause'][one],
            havingBoundaryValue=processedQuery['havingClause'][zero]
        )
        havingOutput = aggOutput.where(havingClause)
        havingOutput = havingOutput.toJSON()
        havingOutput=havingOutput.map(lambda j: Json.loads(j))
        havingOutput=havingOutput.collect()

        '''------GROUPBY HAVING ACTION LOGIC STARTS--------------------'''


        #record the time when the job ends

        self.outputTime = Time.time() - startTime

        '''
            generate a JSON object consising of the result of query and the time taken to perform the query

        '''
        self.sparkOutput = {
            'output': havingOutput,
            'outputTime': self.outputTime,
            'transformations': self.keywords
        }
        return self.sparkOutput

  
  
  
    '''
        The path for the CSV files is provided 

            -list containing the sql actions like WHERE, GROUPBY, SELECT, JOIN etc is created
            -The COLUMN NAMES on which these actions are to be performed are also taken

        This is then passed to the performJob for further processing
    '''

    def importDataset(self, schemaStructure, tables):
        spark = self.createSparkInstance()
        schemaStructure = schemaStructure[tables]
        struct_items = schemaStructure.items()
        schemaList = []
        for x, fieldDatatype in struct_items:
            fdataType = self.fieldDatatype[fieldDatatype]
            schemaList.append(StructField(x, fdataType, Yes))
        schema = StructType(schemaList)
        table = spark.read.csv('/home/astrid/Desktop/flask_assign/Dataset/{table}.csv'.format(table=tables),schema=schema)
        return table
    


    '''
    A spark session is created and the web UI can be accessed at http://127.0.0.1:5000/provideQuery?query=

    The user can put the sql query after the '=' in the URL

    The Spark job then runs to find result after performing the query
    '''
    
    def createSparkInstance(self):
        spark = SparkSession.builder.master('local')
        sparkInstance = spark.appName('runspark')
        sparc = sparkInstance.getOrCreate()
        return sparc

    