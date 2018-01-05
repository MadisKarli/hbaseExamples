package util;

import org.apache.hadoop.hbase.client.Get;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;


/**
 * Created by Madis-Karli Koppel on 29/12/2017.
 */
public class GetFunction implements Function<byte[], Get> {

    private static final Logger logger = LogManager.getLogger(GetFunction.class);

    private static final long serialVersionUID = 1L;

    public Get call(byte[] v) throws Exception {
        return new Get(v);
    }
}
