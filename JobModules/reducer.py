#!/usr/bin/python3
import operator as symbol
import json as Json
import sys as System

zero = 0
one = 1
aggCount = 'count'
tab = '\t'

'''
    The REDUCE script is written to perform the 
        -HAVING operation, 
        -AGGREGATE FUNTION OPERATION 
                                    on the DATASET 
    
    after receiving the processed data from the mapper

'''


class Reducer:

    ''' 
        Logic for performing the HAVING Logic of SQL and 
            filtering data according to the conditions mentioned in the query

        Aggregate operations like SUM, MAX, MIN are also performed, if any.

    '''

    def performOperation(self, havingSymbol, havingBoundaryValue, aggregationFunction, data):
        aggMethod = aggregationFunction
        for attribute in data:
            att_data = data[attribute]
            if aggMethod == aggCount:
                aggregateValue = self.method[aggMethod](att_data)
            else:
                values = [float(value) for value in att_data]
                aggregateValue = self.method[aggMethod](values)
            if self.operate[havingSymbol](aggregateValue, float(havingBoundaryValue)):
                printout = str(attribute) + tab + str(aggregateValue)
                print(printout)


    '''
        1. Takes every records and breaks it down using strip and split
        2. Passes the records to be operated upon and filtered out 
            according to the query requirement


    '''

    def reduce(self):
        for record in System.stdin:
            record = record.strip()
            record = record.split(tab)
            zero_rec = record[zero]
            if zero_rec not in self.data:
                self.data[zero_rec] = [record[one]]
            else:
                self.data[zero_rec].append(record[one])
        self.performOperation(self.havingSymbol, self.havingBoundaryValue, self.aggregationFunction, self.data)


    '''
        initializes variables according to the SQL actions to be performed on the dataset

    '''
    def __init__(self, members):
        self.members = members
        self.aggregationFunction = self.members['aggregationFunction']
        self.data = {}
        self.method = {
            'max': max,
            'sum': sum,
            'min': min,
            'count': len
        }
        self.operate = {
            '=': symbol.eq,
            '<': symbol.lt,
            '>': symbol.gt,
            '<=': symbol.le,
            '>=': symbol.ge,
            '!=': symbol.ne
        }
        self.havingBoundaryValue = self.members['havingBoundaryValue']
        self.havingSymbol = self.members['havingSymbol']


if __name__ == '__main__':
    with open('elements.json', 'r') as elements_file:
        members = Json.load(elements_file)
    reducer = Reducer(members)
    reducer.reduce()