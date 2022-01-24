import os as OperatingSys
zero = 0
one = 1
tab = '\t'
newLine = '\n'

'''
1. Responsible for reading the OUTPUT file at the location specified in the path
    in a folder named part-00000

2.  The generated file is then read record by record and
        sent as queryResult to the Startup.py to be displayed in the UI

3. Directory is then cleaned for next operation to be performed

'''


class outputMAPRED:

    '''
        initializes variables to be used to read and process the output file 
            written by the reducer

    '''
    def __init__(self, configurations, attributes, resultTime):
        self.configurations = configurations
        self.attributes = attributes
        self.outputMapRed = {}
        self.queryResult = []
        self.resultTime = resultTime

    '''
        Fetches the output file written by the reducer and 
            sends the result to be displayed after performing the query

    '''

    def getResult(self):
        command = '/usr/local/hadoop/bin/hdfs dfs -get {hadoopOutputFolder} {localOutputFolder}'.format(hadoopOutputFolder=self.configurations['output_dir'],
                                                                  localOutputFolder=self.configurations['local_output_dir'])
        OperatingSys.system(command)

        '''
            PROVIDE THE FILE PATH HERE FOR GENERATION OF THE OUTPUT FILE

        '''
        localFilePath = '{localOutputFolder}/part-00000'.format(localOutputFolder=self.configurations['local_output_dir'])
        print("MR Result Done!!!!!")

        '''
            The generated file is then read record by record and
                sent as queryResult to the Startup.py to be displayed in the UI

        '''
        with open(localFilePath, 'r') as local_file:
            results = local_file.readlines()

        for result in results:
            result = result.replace(newLine, '')
            result = result.strip()
            result = result.split(tab)
            tempDict = {
                self.attributes[zero]: result[zero],
                self.attributes[one]: result[one]
            }
            self.queryResult.append(tempDict)

        '''
        After performing the mapper and reducer tasks and generating the output file once
            the HDFS directory where the output file was stored
                is cleansed 
                    in order to make space for next operation to be performed

        '''

        clearlocalDirectory = self.configurations['clear_localDirectory']
        clearHDFS = self.configurations['clear_hdfs']
        OperatingSys.system(clearlocalDirectory)
        OperatingSys.system(clearHDFS)
        self.outputMapRed['result'] = self.queryResult

        '''
            Record the time at which the performing job ends
        '''
        self.outputMapRed['resultTime'] = self.resultTime

        return self.outputMapRed
