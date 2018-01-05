package util;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * Created by Madis-Karli Koppel on 29/12/2017.
 */
public class PutFunction implements Function<String, Put> {

    private static final Logger logger = LogManager.getLogger(PutFunction.class);

    private static final long serialVersionUID = 1L;

    public Put call(String v) throws Exception {
        String[] cells = v.split(",");
        Put put = new Put(Bytes.toBytes(cells[0]));

        logger.info("inserting " + Arrays.asList(cells));

        for(int i = 1; i < cells.length; i += 3){
            put.addColumn(Bytes.toBytes(cells[i]), Bytes.toBytes(cells[i + 1]),
                    Bytes.toBytes(cells[i + 2]));
        }


        return put;
    }

}
