from queryLogic import QueryLogic
import os as OperatingSys
import time as Time
import json as Json
from JobModules.hadoopStreaming import hadoopStreaming
from JobModules.outputMAPRED import outputMAPRED
from JobModules.sparkJob import sparkJob
import yaml
zero =0
one =1
comma = ','
openpar =  '('
closepar = ')'


'''
This module is responsible for initiating the 
        -Spark Job
        -MapRed Job
        -Storm Job

After starting up the flask application, the control transfers to this class Startup

Startup is responsible to:

1. Importing the dependencies which defines 
            -the structure of the schema which the dataset must follow, 
            -the configuration for setting up the paths of variables used in this flask app
2. Start the Spark modules,Mapreduce modules, and running the command which starts the Storm Topology.
3. Import the dataset provided by the csv files
    - The location of the csv files can be changed by going to that particular spark,mapreduce,storm files.
4. Collects result by running the HADOOP, SPARK JOB, STORM TOPO MODULES on the IMPORTED DATASET.


'''


class Startup:

    def jobInitiation(self):

        #----------------------------------------------------------------------------------------
        '''
        STEP 1: importing dependencies
        '''

        '''
                DEPENDENCY 1. IMPORTED SCHEMA THAT THE DATASET FOLLOWS
        '''
        with open('Dependencies/schema.yaml', 'r') as schema_file:              
            self.schema = yaml.load(schema_file, Loader=yaml.FullLoader)


        '''
                DEPENDENCY 2. IMPORTED THE PATH REQUIRED BY VARIABLES USED IN THE CODE
        '''
        with open('Dependencies/configurations.yaml', 'r') as config_file:
            self.configurations = yaml.load(config_file, Loader=yaml.FullLoader)


        '''
                DEPENDENCY 3. IMPORTED THE DATATYPE THAT THE ATTRUBUTES OF THE SCHEMA ARE STRUCTURED AS
        '''
        with open('Dependencies/schema_structure.yaml', 'r') as struct_file:
            self.schemaStructure = yaml.load(struct_file, Loader=yaml.FullLoader)

        
        #----------------------------------------------------------------------------------------

        '''
        STEP 2: processing the SQL Query provided by the user in the web interface

                -firstly the SQL Query and the schema are passed to the QueryLogic module
                -the query is then broken down and the actions to be performed are separated such as SELECT, WHERE, GROUPBY, AGGREGATE OPERATIONS
                -the column names and the tables used by each action are kept separately
                -hence the query is translated into the logic that is to be understood by Hadoop and Spark jobs to find an accurate result after breaking down the query
        '''
        
        self.processedQuery, self.queryEle = QueryLogic(self.sqlquery, self.schema).getProcessedQuery()
        selec_method = self.processedQuery['selectMethod']
        group_att = self.processedQuery['groupByAttributes']
        self.attributes = [comma.join(group_att), ' '.join(selec_method[zero])]
        self.tables = self.processedQuery['tables'][zero]


        #----------------------------------------------------------------------------------------
        '''
            STEP 3: Sends the query to be processed using Spark Job
                Spark Job takes in 
                    -configurations
                    -schema
                    -query translated for Spark Job by QueryLogic module
                    -dataset
                
                After aplying the logic of the query and operating on the dataset, the result of the query and the time taken in finding the result is returned
        '''
        
        # start spark job
        print('<================= START SPARK ===============>')
        runspark = sparkJob(self.configurations)
        firstArg = self.processedQuery
        schemeArg =self.schemaStructure
        tableArg = self.tables
        self.queryResultSPARK = runspark.performJob(firstArg, schemeArg, tableArg)
        print('<============== RESULT OF SPARK JOB ==============>')
        print(self.queryResultSPARK)

        #----------------------------------------------------------------------------------------

        '''
            STEP 3: Sends the query to be processed using Hadoop Job
                
                Hadoop Job takes in 
                    -configurations
                    -schema
                    -query translated for Hadoop Job by QueryLogic module
                    -dataset
                
                Using the HADOOP STREAMING, the mapper and reducer jobs can be executed using python scripts

                    - Mapper generates the input csv into <key, value> pairs
                            These <key, value> pairs are then sorted and shuffled and then fed to the Reducer
                    - Reducer generates the final output and then this is written to the HDFS
                
        '''
        

        # start mapreduce job
        print('<============== START MAPREDUCE ============>')
        config_arg = self.configurations
        session = hadoopStreaming(config_arg, tableArg)
        result_performJob = session.performJob()
        outputMapRed = outputMAPRED(config_arg, self.attributes, result_performJob)
        print("MapRed Done!!")
        self.queryResultMR = outputMapRed.getResult()
        print('<============ RESULT OF MAPRED JOB ===========>')
        print(self.queryResultMR)

        #----------------------------------------------------------------------------------------

        '''
            STEP 5: MODULE FOR STORM JOB

                    -Starts the Storm Job by running a command to run a JAVA file
                    -After execution of the Storm Job a JSON object is returned
                    -This JSON object is read by this python script and returned as the query output
                        and the time taken to perform the job.

        strmcmd = 'storm jar {stormTopo_jar} {TopoMain} {arg1} {arg2} {arg3} {arg4} {arg5}'.format(
            stormTopo_jar = self.configurations['stormJar'],
            TopoMain = self.configurations['stormMainPath'],
            arg1 = self.queryEle[1],
            arg2= self.queryEle[2],
            arg3=self.queryEle[3],
            arg4=self.queryEle[4],
            arg5=self.queryEle[0]
        )
        print("Storm Jobs are activated")
        startTime = Time.time()
        OperatingSys.system(strmcmd)
        resultTime = Time.time() - startTime

        f = open('storm_output.json',)
 
        # returns the JSON object which contains time-taken, query results
        self.queryResultSTORM = Json.load(f)
        self.queryResultSTORM.add('timetaken',resultTime)
        f.close()
     '''

        #----------------------------------------------------------------------------------------
        
        '''
            STEP 6: returns the result of the query after performing all the operations in Hadoop and in Spark
        '''

        self.queryResult = {
            'queryResultSPARK': self.queryResultSPARK,
            'queryResultMR': self.queryResultMR,
        }
        #'queryResultSTORM': self.queryResultSTORM
        return self.queryResult


    
    '''
        Creates variables required for the execution of the flask app
    '''
    def __init__(self, sqlquery):
        self.tables = ''
        self.queryResult = {}
        self.processedQuery = {}
        self.sqlquery = sqlquery
        self.schema = {}
        self.queryEle=[] ##
        self.queryResultMR = {}
        self.queryResultSPARK = {}
        self.configurations = {}
        self.schemaStructure = {}
        self.attributes = []
    #self.queryResultSTORM={}