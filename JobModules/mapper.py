#!/usr/bin/python3
import json as Json
import sys as System
import operator as symbol
comma = ','
openpar =  '('
closepar = ')'
tab = '\t'


'''
    The MAPPER script is written to perform the 
        -SELECT operation, 
        -WHERE operation
        -GROUPBY operation
        -AGGREGATION operation if any 
                                    on the DATASET 
    
'''


'''
   MAPPER OUTPUT ================> REDUCER INPUT
'''


class Mapper:


    ''' 
        <key, value> pairs are generated

        According to the conditions mentioned in the query
        Logic for performing the 
        -SELECT Logic
        -GROUPBY logic
        -WHERE Logic
            and
        -Aggregate operations like SUM, MAX, MIN are also performed, if any.

        and the records of the dataset are filtered out accordingly.

        Then sent to the reducer for further processing

    '''
    def execute(self):

        '''
        depending on the size of the input dataset, a large data set is divided into many input-splits. 
        '''
        for record in System.stdin:
            record = record.strip()
            record = record.split(comma)
            symbol = self.whereSymbol
            where_att = self.index_whereAttribute
            where_val = self.whereValue

            '''
            performing the SQL query actions according to the SELECT, GROUPBY, WHERE clauses
                mentioned in the query
                    and the attributes they are to be performed upon
            '''
            if self.operate[symbol](record[where_att], where_val):
                ind_group = self.index_groupby
                groupByAttributes = [record[ind] for ind in ind_group]
                index_method = self.index_selectMethodAttribute
                aggregationAttribute = record[index_method]
                groupByAttributes = comma.join(groupByAttributes)
                printout = groupByAttributes + tab + str(aggregationAttribute)
                print(printout)

    '''
        initializes variables according to the SQL actions to be performed on the dataset
            by the MAPPER

    '''
 
    def __init__(self, member):
        self.index_selectAttribute = member['index_selectAttribute']
        self.whereValue = member['whereValue']
        self.whereSymbol = member['whereSymbol']
        self.index_selectMethodAttribute = member['index_selectMethodAttribute']
        self.operate = {
            '<=': symbol.le,
            '>=': symbol.ge,
            '!=': symbol.ne,
            '=': symbol.eq,
            '<': symbol.lt,
            '>': symbol.gt
        }
        self.index_groupby = member['index_groupby']
        self.index_whereAttribute = member['index_whereAttribute']

if __name__ == '__main__':
    with open('elements.json', 'r') as ele_file:
        members = Json.load(ele_file)
    mapper = Mapper(members)
    mapper.execute()