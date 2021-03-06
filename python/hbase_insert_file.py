from pyspark import SparkContext, SparkConf

import HBaseController
import hashlib

import argparse

# for reading sequence files: key, value pairs that have data like:
# \00=<http::www.bestpractice.ee::/robots.txt::null::20150215040001\00\00\00\009\00\0

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input", help="path to input", required=True)
parser.add_argument("-t", "--table_name", help="table name in hbase", default='koppel_test')
parser.add_argument("-hb", "--hbase_location", help="HBase ip", default='ir-hadoop1')
parser.add_argument("-bs", "--batch_size",
                    help="batch size for hbase table. Shows how many values it inserts at one time",
                    default=100, type=int)

args = parser.parse_args()

filename = args.input
table_name = args.table_name
hbase_location = args.hbase_location
batch_size = args.hbase_location


def send_partition(parts):
    """
    :param parts: rdd.foreachPartition result where rdd contains key value pairs to be inserted into HBase
    :return: None
    Creates a new HBaseController for every partition
    Can be optimized by using a Pool controllers (connections)
    """
    hc = HBaseController.HBaseController(hbase_location, table_name, batch_size)

    for part in parts:
        hc.insert_batch(part[0], part[1])

    hc.batch.send()
    hc.stop()


def map_key_value(row):
    id_parts = row[0].split("::")
    try:
        return row[0], {'domain:main': id_parts[1], 'domain:sub': id_parts[2], 'text:all': row[1]}
    except IndexError:
        return row[0], {'text:all': row[1]}


# Start Spark
conf = SparkConf().setAppName("Hbase insert")
sc = SparkContext(conf=conf)

print "Spark Started"
print "Reading data from: " + filename

# Read the files
reader = sc.sequenceFile(filename, "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text")

# Map the files into RDD that has key, value for inserting into HBase
data2 = reader.map(map_key_value)

# Insert RDD into HBase, partition by partition
data2.foreachPartition(send_partition)

print('done')
sc.stop()
