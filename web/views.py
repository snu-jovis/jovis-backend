import psycopg2

from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response

class QueryView(APIView):
    def post(self, request, format=None):
        # SQL 공격이 근본적으로 가능하므로, 절대 링크를 외부공개 하지 마세요.
        q = request.data.get('query', 'EXPLAIN SELECT \'Hello World\'')
        
        # get query results
        conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")    # Connect to your postgres DB
        cur = conn.cursor()     # Open a cursor to perform database operations
        cur.execute(q)        # Execute a query
        records = cur.fetchall()     # Retrieve query results

        # return
        return Response({'result': str(records)})
