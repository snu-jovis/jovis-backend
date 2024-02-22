import psycopg2
import os
import time

from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response

from backend.settings import PG_LOG_FILE, PG_LOG_BACKUP_DIR

def clear_previous_log():
    os.system(f"cp {PG_LOG_FILE} {PG_LOG_BACKUP_DIR}/{time.time()}_prev")
    os.system(f"echo '' > {PG_LOG_FILE}")

def read_and_clear_log():
    filename = f"{PG_LOG_BACKUP_DIR}/{time.time()}_pq"
    os.system(f"cp {PG_LOG_FILE} {filename}")
    os.system(f"echo '' > {PG_LOG_FILE}")

    ret = None
    with open(filename, 'r') as f:
        ret = '\n'.join(f.readlines())

    return ret

class QueryView(APIView):
    def post(self, request, format=None):
        # SQL 공격이 근본적으로 가능하므로, 절대 링크를 외부공개 하지 마세요.
        q = request.data.get('query', 'EXPLAIN SELECT \'Hello World\'')
        
        # get query results
        conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")    # Connect to your postgres DB
        cur = conn.cursor()     # Open a cursor to perform database operations

        clear_previous_log()

        cur.execute(q)        # Execute a query
        records = cur.fetchall()     # Retrieve query results

        log = read_and_clear_log()

        # return
        return Response({'result': str(records), 'log': log})
