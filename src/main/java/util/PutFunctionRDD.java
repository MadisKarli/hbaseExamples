package util;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


/**
 * Created by Madis-Karli Koppel on 13/01/2018.
 */
public class PutFunctionRDD implements Function<Tuple2<String, String>, Put> {

    private static final Logger logger = LogManager.getLogger(PutFunctionRDD.class);

    private static final long serialVersionUID = 1L;

    @Override
    public Put call(Tuple2<String, String> stringStringTuple2) throws Exception {
        // TODO MOVE TO SAME HASH AS PYTHON
        Put put = new Put(Bytes.toBytes(stringStringTuple2._1.hashCode()));

        put.addColumn(Bytes.toBytes("raw"), Bytes.toBytes("id"), Bytes.toBytes(stringStringTuple2._1));
        put.addColumn(Bytes.toBytes("raw"), Bytes.toBytes("data"), Bytes.toBytes(stringStringTuple2._2));
        return put;
    }
}
