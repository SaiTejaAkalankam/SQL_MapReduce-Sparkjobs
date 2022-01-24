import json as Json

KEYS = ['select', 'from', 'where', 'group', 'having']
zero = 0
one = 1
By = 'by'
comma = ','
openpar =  '('
closepar = ')'
equalsTo = '='
notEqualsTo ='!='
grtEquals = '>='
lessEquals = '<='
grtThan='>'
lessThan='<'

'''
    The received SQL Query is

        STEP1: 
                processQuery
                Fetches the SQL query
                    -removes extra spaces
                    -breaks it word by word
                    -stores the keywords/actions into memberList

        STEP2: FETCH the processed SQL Query using - getProcessedQuery

        STEP3: BREAKDOWN QUERY using - breakdownQuery into keywords like SELECT, FROM TABLE, WHERE, GROUP BY, HAVING

        STEP4: MAPPING Query elements to their respective variables which are further processed
                    -format for "elements.json" which is the return JSON object is also defined here

        STEP5: Applying the logic according to the keywords in the quer
                    -fromLogic
                    -selectLogic
                    -groupbyLogic
                    -havingLogic
                    -whereLogic
        STEP6: Returns the processed query to Startup for being operated on by different Jobs

'''



class QueryLogic:

    '''
        It fetches the SQL query
            -removes extra spaces
                -breaks it word by word
                    -stores the keywords/actions into memberList

    '''

    def processQuery(self):
        sqlquery = self.sqlquery
        sqlquery = sqlquery.strip()
        sqlquery = sqlquery.split()
        sqlquery.remove(By)
        memberList = []
        members = ''
        for item in sqlquery[one:]:
            if item.lower() in KEYS:
                memberList.append(members)
                members = ''
            else:
                members = members + item
        memberList.append(members)
        return memberList


    #-----------------------------------------------------------------------------
    
    '''
        For selection of the DATASET Tables that the query wants to process upon

    '''    
    
    def fromLogic(self, members):
        if comma in members:
            members = members.split(comma)
            self.tables = [member.strip() for member in members]
        else:
            self.tables.append(members.strip())
        self.processedQuery['tables'] = self.tables


    #-----------------------------------------------------------------------------
    '''
        Logic for breaking down a query which starts with the SELECT sql keyword
            it operates on the column names that need to be select
            it can also consist of AGGREGATE FUNCTIONS

        eg: SELECT * - for selecting all columns of the given table
            SELECT AGGREGATE FUNCTION(ColumnX)
            SELECT col1, col2

    '''        

    def selectLogic(self, members):
        if comma in members:
            members = members.split(comma)
            for member in members:
                if openpar not in member:
                    self.selectAttributes.append(member.strip())
                else:
                    firstvar = member.split(openpar)[zero].lower().strip()
                    secondvar = member.split(openpar)[one][:-1].strip()
                    tempList = [firstvar, secondvar]
                    self.selectMethod.append(tempList)
        else:
            member = members
            if openpar not in member:
                self.selectAttributes.append(member.strip())
            else:
                firstvar = member.split(openpar)[zero].lower().strip()
                secondvar = member.split(openpar)[one][:-1].strip()
                self.selectMethod.append(firstvar)
                self.selectMethod.append(secondvar)

        self.processedQuery['selectAttributes'] = self.selectAttributes
        self.processedQuery['selectMethod'] = self.selectMethod


    #-----------------------------------------------------------------------------

    '''
        Logic for breaking down a query which consists of a GROUP BY condition

    '''    
    def groupbyLogic(self, members):
        if comma in members:
            members = members.split(comma)
            self.groupByAttributes = [member.strip() for member in members]
        else:
            self.groupByAttributes.append(members.strip())

        self.processedQuery['groupByAttributes'] = self.groupByAttributes

    #-----------------------------------------------------------------------------
    '''
        Logic for breaking down a query which consists of a HAVING clause

    '''    

    def havingLogic(self, members):
        items = [lessEquals, grtEquals, notEqualsTo, equalsTo, grtThan, lessThan]
        for item in items:
            if item in members:
                boundaryValue = members.split(item)[1]
                boundaryValue=boundaryValue.strip()
                boundaryValue=boundaryValue.lower()
                self.havingClause = [boundaryValue, item]
                break
        self.processedQuery['havingClause'] = self.havingClause

    #-----------------------------------------------------------------------------

    '''
        Logic for breaking down a query which consists of a WHERE condition

    '''    

    def whereLogic(self, members):
        items = [lessEquals, grtEquals, notEqualsTo, equalsTo, grtThan, lessThan]
        for item in items:
            if item in members:
                members = members.strip()
                members=members.split(item)
                whereValue = members[one].strip()
                whereAttribute = members[zero].strip()
                self.processedQuery['whereAttribute'] = whereAttribute
                self.processedQuery['whereSymbol'] = item
                self.processedQuery['whereValue'] = whereValue.strip('"')
                break


    #-----------------------------------------------------------------------------
    '''
        Fetches the memberList which consists of the broken down SQL query keyword by keyword
            Performs mapping of the query
    '''

    def getProcessedQuery(self):
        self.breakdownQuery()
        self.mapQueryElements()
        return self.processedQuery,self.memberlist
    
    #-----------------------------------------------------------------------------
    '''
        breaks down Query into keywords like SELECT, FROM TABLE, WHERE, GROUP BY, HAVING 
            and stores it into memberList

    '''
    def breakdownQuery(self):
        memberList = self.processQuery()
        self.memberlist = memberList #
        self.fromLogic(memberList[one])
        self.groupbyLogic(memberList[3])
        self.selectLogic(memberList[zero])
        self.havingLogic(memberList[4])
        self.whereLogic(memberList[2])

    #-----------------------------------------------------------------------------

    '''

        Maps Query elements to their respective variables which are further processed

    '''

    def mapQueryElements(self):

        query= self.processedQuery
        selec_method = query['selectMethod']
        tables = query['tables'][zero]
        attributes = list(self.schema[tables])

        #storing the column names which are to be operated upon by SELECT Logic
        for attribute in query['selectAttributes']:
            ind_attr = attributes.index(attribute)
            self.index_selectAttribute.append(ind_attr)
        print(len(selec_method))
        index_selectMethodAttribute = attributes.index(selec_method[zero][one])
        aggregationFunction = selec_method[zero][zero]

        #the column names to be used for GROUPBY Logic
        for attribute in query['groupByAttributes']:
            ind_attr = attributes.index(attribute)
            self.index_groupby.append(ind_attr)
        index_whereAttribute = attributes.index(query['whereAttribute'])
        havingSymbol = query['havingClause'][one]
        havingBoundaryValue = query['havingClause'][zero]
        whereValue = query['whereValue']
        whereSymbol = query['whereSymbol']


        '''
            Structure for the JSON format to be returned by the job

        '''
        members = {
            'index_selectAttribute': self.index_selectAttribute,
            'aggregationFunction': aggregationFunction,
            'index_selectMethodAttribute': index_selectMethodAttribute,
            'tables': tables,
            'index_whereAttribute': index_whereAttribute,
            'whereValue': whereValue,
            'whereSymbol': whereSymbol,
            'index_groupby': self.index_groupby,
            'havingSymbol': havingSymbol,
            'havingBoundaryValue': havingBoundaryValue
        }

        with open('Dependencies/elements.json', 'w') as target:
            Json.dump(members, target)


    '''
        Creates variables required for the processing the SQL query and translating 
            it in order to be understood by
                -Storm Job
                -Hadoop Job
                -Spark Job of the flask app
    '''        
    
    def __init__(self, sqlquery, schema):
        self.selectAttributes = []
        self.selectMethod = []
        self.index_groupby = []
        self.index_selectAttribute = []
        self.processedQuery = {}
        self.sqlquery = sqlquery
        self.tables = []
        self.groupByAttributes = []
        self.havingClause = []
        self.schema = schema
        self.memberlist=[]