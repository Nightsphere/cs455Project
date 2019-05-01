package cs455.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Cleaner {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("College Cleaner")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        Dataset<Row> db = sparkSession.read().format("csv").option("header", true).load(args[0]);

        JavaRDD<String> colleges = db.toJavaRDD().map(row -> {
            String result = row.getString(0) + "," + row.getString(3) + "," + row.getString(5) + "," +
                    row.getString(378) + "," + row.getString(379) + "," + row.getString(1509) + "," +
                    row.getString(1510) + "," + row.getString(1394) + "," + row.getString(61) + ", " +
                    row.getString(63) + ", " + row.getString(65) + "," + row.getString(69) + "," +
                    row.getString(70) + "," + row.getString(75) + "," + row.getString(78) + "," +
                    row.getString(79) + "," + row.getString(96) + "," + row.getString(83) + "," +
                    row.getString(86) + "," + row.getString(87) + "," + row.getString(91) + "," +
                    row.getString(95) + "," + row.getString(97) + "," + row.getString(74) + "," +
                    row.getString(67);
            return result;
        });

        colleges.saveAsTextFile("/cleaned");

        sparkSession.stop();
        sc.stop();
    }
}
