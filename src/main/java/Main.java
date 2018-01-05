import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import util.GetFunction;
import util.PutFunction;
import util.ResultFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Madis-Karli Koppel on 27/12/2017.
 */
public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    private static JavaSparkContext jsc;

    private static Configuration hConf;

    private static Connection connection;

    private static Table table;

    private static String tableName;


    public static void main(String[] args) throws IOException {

        tableName = "Test5";
        hConf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(hConf);
        table = connection.getTable(TableName.valueOf(tableName));

        createJava();

        insertJava();

        getJava(5);

        deleteJava("2");

        getJava(5);

        startSpark();

        List<String> values = new ArrayList<String>();
        values.add("1,name,First,Igor,name,Last,Smigor,id,personal,123");

        insertSpark(values);

        getSparkRegular();

        getSparkStreaming(5);

        stopSpark();

        table.close();
        connection.close();
    }

    private static void createJava() throws IOException {

        Admin admin = connection.getAdmin();

        logger.info("Checking if table " + tableName + " can be created...");

        if (!admin.tableExists(TableName.valueOf(tableName))) {
            logger.info("No table with this name, creating: " + tableName);
            HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
            hbaseTable.addFamily(new HColumnDescriptor("id"));
            hbaseTable.addFamily(new HColumnDescriptor("name"));
            admin.createTable(hbaseTable);
            logger.info("Table created...");
        } else {
            logger.info("There already exists a table with name:" + tableName);
        }
    }

    private static void insertSpark(List<String> values) {

        logger.info("Inserting into " + tableName + " values " + values);

        JavaRDD<String> rdd = jsc.parallelize(values);

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hConf);

        logger.info("connected to hbase");

        hbaseContext.bulkPut(rdd,
                TableName.valueOf(tableName),
                new PutFunction());
    }

    private static void insertJava() throws IOException {
        logger.info("inserting into table " + tableName);
        String[][] people = {
                {"7", "Marcel", "Haddad", "marcel@xyz.com", "M", "26"},
                {"2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24"},
                {"3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27"},
                {"4", "Rae", "Schroeder", "rae@xyz.com", "F", "31"},
                {"5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25"},
                {"6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24"}};

        for (int i = 0; i < people.length; i++) {
            Put person = new Put(Bytes.toBytes(people[i][0]));
            person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("First"), Bytes.toBytes(people[i][1]));
            person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("Last"), Bytes.toBytes(people[i][2]));
            person.addColumn(Bytes.toBytes("id"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
            table.put(person);
        }

        logger.info("Inserted into " + tableName + " values " + Arrays.asList(people));
    }

    private static void getJava(int numberOfRows) throws IOException {

        logger.info("Fetching " + String.valueOf(numberOfRows) + " rows from table " + tableName);

        List<Get> queryRowList = new ArrayList<Get>();

        for (int i = 1; i <= numberOfRows; i++) {
            queryRowList.add(new Get(Bytes.toBytes(String.valueOf(i))));
        }

        Result[] results = table.get(queryRowList);
        for (Result r : results) {
            byte[] value = r.getValue(Bytes.toBytes("name"), Bytes.toBytes("First"));
            byte[] value1 = r.getValue(Bytes.toBytes("name"), Bytes.toBytes("Last"));

            String firstName = Bytes.toString(value);
            String lastName = Bytes.toString(value1);

            logger.info("name: " + firstName + " " + lastName);
        }
    }

    private static void getSparkRegular() throws IOException {

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hConf);

        Scan scan = new Scan();
        scan.setCaching(100);

        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan);

        System.out.println("Number of Records found : " + hbaseRdd.count());

        JavaRDD<String> result = hbaseRdd.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
            public String call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
                return new ResultFunction().call(tuple2._2);

            }
        });

        logger.error("Regular get result" + result.collect().toString());
    }

    private static void getSparkStreaming(int numberOfRows) {

        logger.info("Fetching " + String.valueOf(numberOfRows) + " rows from table " + tableName);

        List<byte[]> list = new ArrayList<byte[]>(numberOfRows);
        for (int i = 1; i <= numberOfRows; i++) {
            list.add(Bytes.toBytes(String.valueOf(i)));
        }

        JavaRDD<byte[]> rdd = jsc.parallelize(list);

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hConf);

        JavaRDD<String> result = hbaseContext.bulkGet(TableName.valueOf(tableName), 2, rdd, new GetFunction(),
                new ResultFunction());

        logger.info("Streaming get result " + result.collect().toString());
    }

    private static void deleteJava(String rowId) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowId));
        table.delete(delete);
        logger.info("Deleted row id:" + rowId);
    }

    private static void startSpark() {
        SparkConf sparkConf = new SparkConf().setAppName("j").set("spark.executor.memory", "512m").set("spark.driver.memory", "512m");
        jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");
    }

    private static void stopSpark() {
        jsc.stop();
    }
}
