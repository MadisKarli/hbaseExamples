package util;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Created by Madis-Karli Koppel on 29/12/2017.
 */
public class ResultFunction implements Function<Result, String> {

    private static final Logger logger = LogManager.getLogger(ResultFunction.class);

    private static final long serialVersionUID = 1L;

    public String call(Result result) throws Exception {
        List<String> resultList = new LinkedList<String>();

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = result.getMap();

        try {
            for (byte[] k : navigableMap.keySet()) {

                String key1 = Bytes.toString(k);
                NavigableMap<byte[], NavigableMap<Long, byte[]>> map1 = navigableMap.get(k);

                for (byte[] k2 : map1.keySet()) {

                    String key2 = Bytes.toString(k2);
                    NavigableMap<Long, byte[]> map2 = map1.get(k2);

                    for (Long k3 : map2.keySet()) {

                        String value = Bytes.toString(map2.get(k3));
                        String combined = key1 + ":" + key2 + ":" + value;
                        resultList.add(combined);
                    }
                }
            }
        } catch(NullPointerException e){
            // When we use ID that is not in table
        }

        return resultList.toString();
    }
}