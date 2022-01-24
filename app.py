
'''
BITS PILANI - MTECH COURSE CS/SS G527
ASSIGNMENT 1 (check assignment 1 pdf)

done by

AKALANKAM SAI TEJA - 2020H1030142P
RIYA PRITWANI - 2020H1030137P
ANSA AHMED - 2020H1120303P
DEBJANI GHOSH - 2020H1120263P 

'''

import flask
from flask import request, jsonify
from startup import Startup

app = flask.Flask(__name__)
app.configurations["DEBUG"] = True

'''
1. api_all is the fucntion responsible for starting up the flask application execution
2. The user is expected to setup the environment by installing pyspark and hadoop for execution of this flask application
3. The user can open the web browser http://127.0.0.1:5000/provideQuery?sqlquery= and type in the query after the equal to '=' sign
4. Format of the sql query can be understood by the following example
            http://127.0.0.1:5000/provideQuery?sqlquery=select userid, age, count(userid) from users where age = 32 group by gender having count(userid)>2

'''


@app.route('/provideQuery', methods=['GET'])
def api_all():

    #return("hello world")
    sqlquery_parameters = request.args
    sqlquery = sqlquery_parameters.get('sqlquery')
    #return (sqlquery)
    startup = Startup(sqlquery)
    result = startup.jobInitiation()
    return jsonify(result)                  
    
    ''' returns the json object as ELEMENTS.JSON file consisting of the output of the query and the time taken to return the result
    '''


if __name__ == '__main__':
    app.run()