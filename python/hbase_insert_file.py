import sys
from pyspark import SparkContext, SparkConf

import os
import HbaseController

# stupid workaround for my local pc
os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-8-openjdk-amd64/jre"


filename = sys.argv[1]

table_name = b'koppel_test'
batch_size = 100
# for reading sequence files: key, value pairs that have data like:
# \00=<http::www.bestpractice.ee::/robots.txt::null::20150215040001\00\00\00\009\00\0
textOnly = False
# for reading the plaintext files that contain dictionary data like:
# {'uncrossMatchedEntities_ORG': [(u'main', 2)], 'singleMatchedEntities_PER': [], 'crossMatchedEntities_PER': [] ...
estnltkOutput = not textOnly


# Start Spark
conf = SparkConf().setAppName("Hbase insert").setMaster("local")
sc = SparkContext(conf=conf)

hc = HbaseController.HbaseController('localhost', table_name, batch_size)


if textOnly:
    hc.schema = ["raw:id", "raw:data"]
    # Read a file
    reader = sc.sequenceFile(filename, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")

    # TODO convert to inserting in map phase
    print "inserting to table ", hc.table_name
    for x in reader.collect():
        hc.insert_batch(x)


if estnltkOutput:
    # TODO convert to inserting in map phase
    reader = sc.textFile(filename)

    # Map the file to tuple
    # Here insert to table could also be used but I get cython exception
    data = reader.map(lambda l: eval(l))

    # THIS IS BAD, THIS IS VERY BAD!
    # this should be in map but in map I receive an error
    # Bad because it actually collects the results
    # If we could do it in map then we would get faster runtime
    print "inserting to table ", hc.table_name
    for x in data.collect():
        hc.insert_batch_dict(x)



# print(data.take(5))


# send all that did not fit into batch
hc.batch.send()

hc.read_table()

# Safely remove hardware: Spark edition
print('done')
hc.stop()
sc.stop()
