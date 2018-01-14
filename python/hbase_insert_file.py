import sys
from pyspark import SparkContext, SparkConf

import os
import HbaseController

# stupid workaround for my local pc
os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-8-openjdk-amd64/jre"


filename = sys.argv[1]

table_name = b'koppel_test'
batch_size = 100

# Start Spark
conf = SparkConf().setAppName("Hbase insert").setMaster("local")
sc = SparkContext(conf=conf)

hc = HbaseController.HbaseController('localhost', table_name, batch_size)
hc.schema = ["raw:id", "raw:data"]

# Create a new table
# hc.conn.create_table(
#     table_name,	{'raw': dict()}
# )


# Read a file
# newAPIHadoopFile can be also used
reader = sc.sequenceFile(filename, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")

# Map the file to tuple
# Here insert to table could also be used but I get cython exception
data = reader.map(lambda z: (z[0], z[1]))

# THIS IS BAD, THIS IS VERY BAD!
# this should be in map but in map I receive an error
# Bad because it actually collects the results
# If we could do it in map then we would get faster runtime
for x in reader.collect():
    hc.insert_batch(x)

# print(data.take(5))


# send all that did not fit into batch
hc.batch.send()
hc.read_table()

