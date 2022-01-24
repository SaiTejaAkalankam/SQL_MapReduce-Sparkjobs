import os as OperatingSys
import time as Time


'''

    hadoopStreaming runs the MapReduce job on the Hadoop cluster using the Hadoop Streaming API. 
    It then returns the time taken to complete the MapReduce job.

'''


class hadoopStreaming:

    def performJob(self):

        '''

            After starting up the HDFS system, the Hadoop Streaming command can be given in the following format
                hadoop jar <path of jar file> -input <path of input csv file>
                    -output <path of output file to be generated>
                    -mapper <path of mapper file>
                    -reducer <path of reducer file>

        '''
        command = '/usr/local/hadoop/bin/hadoop jar {stream_jarfile} -file {path_of_element} -file {path_of_mapper} ' \
              '-mapper mapper.py -file {path_of_reducer} -reducer reducer.py -input {input_dir}/{table}.csv -output ' \
              '{output_dir}'.format(
            stream_jarfile=self.configurations['stream_jarfile'],
            path_of_element=self.configurations['path_of_element'],
            path_of_mapper=self.configurations['path_of_mapper'],
            path_of_reducer=self.configurations['path_of_reducer'],
            input_dir=self.configurations['input_dir'],
            table=self.tables,
            output_dir=self.configurations['output_dir']
            )


        #record the starting time of the job
        startTime = Time.time()
        OperatingSys.system(command)
        #record the finishing time of the hadoop job
        resultTime = Time.time() - startTime
        return resultTime
    

    def __init__(self, tables, configurations):
        self.tables = tables
        self.configurations = configurations