from pyspark import SparkContext, SparkConf

import HBaseController
from ast import literal_eval as make_tuple

import argparse

# for reading the plaintext files that contain dictionary data like:
# {'uncrossMatchedEntities_ORG': [(u'main', 2)], 'singleMatchedEntities_PER': [], 'crossMatchedEntities_PER': [] ...

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input", help="path to input", required=True)
parser.add_argument("-t", "--table_name", help="table name in hbase", default='koppel_test2')
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


def map_dict(row):
    t = make_tuple(row)
    values = eval(str(t[1]))

    out = {}
    for k, v in values.iteritems():
        if k == "hostname":
            out['domain:main'] = str(v)
        elif k == "path":
            out['domain:sub'] = str(v)
        else:
            out['text:' + k] = str(v)

    return str(t[0]), out


# Start Spark
conf = SparkConf().setAppName("Hbase insert")
sc = SparkContext(conf=conf)

print "Spark Started"
print "Reading data from: " + filename

# Read the files
reader = sc.textFile(filename)

# Map every line into an RDD containing dictionary ({})
data = reader.map(map_dict)

data.foreachPartition(send_partition)

print('done')
sc.stop()
