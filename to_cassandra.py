from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def save_to_cassandra(currunt_df, epoc_id):
    print("printing epoc_id: ")
    print(epoc_id)

    print("Printing before Cassandra table save: " + str(epoc_id))
    currunt_df \
    .write\
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table=cassandra_table_name, keyspace=cassandra_keyspace_name) \
    .save()
    print("Printing after cassandra table save: " + str(epoc_id))