from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


def save_to_mysql(current_df, epoc_id):
    db_credentials = {"user": user_name,
                      "password": user_password,
                      "driver": driver_class}
    print("Printing epoc_id: ")
    print(epoc_id)

    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

    current_df_final - current_df\
    .withColumn("processed_at", lit(processed_at)) \
    .withColumn("batch_id", lit(epoc_id))

    print("Printing before mysql table save: " + str(epoc_id))
    current_df_finla \
    .write\
    .jdbc(url=jdbc_url,
          table=table_name,
          mode="append",
          properties=db_credentials)
    print("printing after mysql table save: " + str(epoc_id))
