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
    delta = datetime.datetime.now() - datetime.timedelta(minutes=interval)
    return int(time.mktime(delta.timetuple()))


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
    return {
      "query": {
        "bool": {
          "must": [
            {
                "range":
                    {"AvgTicketPrice":
                    {
                        "gte": 400, "lte": 500
                    }
                
             }
            },
            {
              "range": {
                "timestamp": {
                  "gte": get_timestamp(500),
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
        cursor.execute(sql_query, (data["FlightNum"], data["OriginCityName"], data["timestamp"], data["AvgTicketPrice"]))
    sql_conn.commit()


def get_sql_query():
    return "INSERT INTO FlightData1 (num, name, timestamp, price) values (%s, %s, %s, %s)"


def main():
    es_conn = get_es_conn()
    es_query = get_es_query()
    es_result = es_conn.search(index=CONFIG["elasticsearch"]["index"], body=es_query, size=10, sort='AvgTicketPrice:desc')
    sql_query = get_sql_query()
    sql_conn = get_mysql_conn()
    update_table(sql_conn, sql_query, es_result)


if __name__ == "__main__":
#    import pdb;pdb.set_trace()
    main()

