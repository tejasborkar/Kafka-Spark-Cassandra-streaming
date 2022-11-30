from pyspark.sql import SparkSession
from to_cassandra import save_to_cassandra
from to_mysql import save_to_mysql
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# kafka configuration
topic_name = "datathrow"
bootstap_server = "localhost:9092"
data_file_path = "file://home/hdfs/project/data/sales_data.csv"

# mysql configuration
host_name = "localhost"
port_no = "3306"
database_name = "sales_db"
driver_class = "com.mysql.jdbc.Driver"
table_name = "all_sales_data"
user_name = "root"
user_password = "hadoop"
jdbc_url = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name

# cassandra configuration
cassandra_host_name = "localhost"
cassandra_port_no = "9042"
cassandra_keyspace_name = "sparkcode"
cassandra_table_name = "sales_table"

if_name_ = "__main__":
   print("Data Provessing Aplication Started ....")
   print(time.strftime("%Y-%m-%d %H:%M:%S"))

   spark = sparksession \
       .builder \
       .appName("Kafka Spark Streaming") \
       .master("locla[*]") \
       .config("spark.jars", "file://home/hdfs/project/pyspark/lib/jsr166e-1.1.0.jar, file:///home/hdfs/project/pyspark/lib/spark-cassandra-connector-2.4.0-s_2.11.jar,file:///usr/share/java/mysql-connector-java-5.1.45.jar,file:///home/hdfs/project/pyspark/lib/spark-sql-kafka-0-10_2.11-2.4.4.jar,file:///home/hdfs/project/pyspark/lib/kafka-clients-1.1.0.jar") \
       .config("spark.executor.extraClassPath", "file://home/hdfs/project/pyspark/lib/jsr166e-1.1.0.jar:file://home/hdfs/project/pyspark/lib/spark-cassandra-2.4.0-s_2.11.jar:file:///usr/share/java/mysql-connector-java-5.1.45.jar,file:///home/hdfs/project/pyspark/lib/spark-sql-kafka-0-10_2.11-2.4.4.jar:file:///home/hdfs/project/pyspark/lib/kafka-clients-1.1.0.jar")
       .config("spark.executor.ectraLibrary", "file://home/hdfs/project/pyspark/lib/jsr166e-1.1.0.jar:file://home/hdfs/project/pyspark/lib/spark-cassandra-2.4.0-s_2.11.jar,file:///usr/share/java/mysql-connector-java-5.1.45.jar,file:///home/hdfs/project/pyspark/lib/spark-sql-kafka-0-10_2.11-2.4.4.jar:file:///home/hdfs/project/pyspark/lib/kafka-clients-1.1.0.jar")
       .config("spark.driver.extraClassPath", "file://home/hdfs/project/pyspark/lib/jsr166e-1.1.0.jar:file://home/hdfs/project/pyspark/lib/spark-cassandra-2.4.0-s_2.11.jar,file:///usr/share/java/mysql-connector-java-5.1.45.jar,file:///home/hdfs/project/pyspark/lib/spark-sql-kafka-0-10_2.11-2.4.4.jar:file:///home/hdfs/project/pyspark/lib/kafka-clients-1.1.0.jar" )
       .config("spark.cassandra.connection.host", cassandra_host_name)\
       .config("spark.cassandra.connection.prot", cassandra_port_no) \
       .getOrCreate()

spark.sparkContext.setLofLevel("ERROR")

orders_df = spark \
    .readStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers",kafka_bootstrap_servers)\
    .option("subscribe", kafka_topic_name)\
    .option("startingOffsets", "latest") \
    .load()

print("Printing schema of orders_df: ")
orders_df.printSchema()

orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

orders.schema = StructType()\
    .add("order_id", StringType())\
    .add("created_at", StringType())\
    .add("discount", StringType())\
    .add("product", StringType())\
    .add("quantity", StringType())\
    .add("order_id", StringType())\
    .add("subtotal", StringType())\
    .add("tax", StringType())\
    .add("customer_id", StringType())\

orders_df2 = orders_df1\
    .select(from_json(col("value"), orders_schema).alias("orders"),"timestamp")

orders_df3 = orders_df2.select("orders.*", "tiemstamp")

orders_df3\
    .writeStream\
    .trigger(processingTime='15 seconds')\
    .outputMode("update")\
    .foreachBatch(save_to_cassandra)\
    .start()

customers_df = spark.read.csv(customers_data_file_path, header=True, inferSchema=True)
customers_df.printSchema()
customers_df.show(5, False)

orders_df4 = orders_df3.join(customers_df, orders_df2.customer_id == customers_df.customer_id, how = 'inner')
print("printing scheam of orders_df4")
orders_df4.printScheam()

orders_df5 = orders_df4.groupBy("source","state")\
    .agg({'total':'sum'}).select({"source", "state", col("sum(total)").alias("total_sum_amount")})

print("Printing Schema of orders_dfs: ")
orders_df5.printScheam()

trans_detail_write_stream = orders_dfs\
    .writeStream\
    .trigger(processingTime='15 seconds')\
    .outputMode("update")\
    .foreachBatch(save_to_mysql)\
    .start()

trans_detail_write_stream.awaitTermination()

print("Pyspark straming with kafka completed")