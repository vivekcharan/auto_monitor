import datetime
import time
from elasticsearch import Elasticsearch
import mysql.connector


CONFIG = {
    "elasticsearch": {
        "host": "192.168.28.16",
        "index": "kibana_sample_data_flights"
    },
    "mysql": {
        "host": "192.168.28.16",
        "user": "root",
        "password": "root",
        "database": "test",
        "port": 3306
    }
}


def get_timestamp(interval):
    current = datetime.datetime.now()
    delta = current - datetime.timedelta(minutes=interval)
    return int(time.mktime(delta.timetuple())) * 1000, int(time.mktime(current.timetuple())) * 1000


def get_es_conn():
    return Elasticsearch(CONFIG["elasticsearch"]["host"])


def get_mysql_conn():
    return mysql.connector.connect(
        host=CONFIG["mysql"]["host"],
        user=CONFIG["mysql"]["user"],
        password=CONFIG["mysql"]["password"],
        port=CONFIG["mysql"]["port"],
        db=CONFIG["mysql"]["database"]
    )


def get_es_query():
    gte, lte = get_timestamp(240)
    return {
      "query": {
        "bool": {
          "must": [
            {
              "query_string": {
                "query": "Cancelled:true"
              }
            },
            {
              "range": {
                "timestamp": {
                  "gte": gte,
                  "lte": lte,
                  "format": "epoch_millis"
                }
              }
            }
          ]
        }
      }
    }


def update_table(sql_conn, sql_query, es_result):
    cursor = sql_conn.cursor()
    for row in es_result["hits"]["hits"]:
        data = row["_source"]
        cursor.execute(sql_query, (data["FlightNum"], data["timestamp"]))
    sql_conn.commit()


def get_sql_query():
    return "INSERT INTO FlightData (num, timestamp) values (%s, %s)"


def main():
    es_conn = get_es_conn()
    es_query = get_es_query()
    es_result = es_conn.search(index=CONFIG["elasticsearch"]["index"], body=es_query, size=9999)
    sql_query = get_sql_query()
    sql_conn = get_mysql_conn()
    update_table(sql_conn, sql_query, es_result)


if __name__ == "__main__":
#    import pdb;pdb.set_trace()
    main()
