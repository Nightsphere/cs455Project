package cs455.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.TreeMap;

import static org.apache.spark.sql.functions.col;

public class Driver {

    private static long totalEarnings = 0;
    private static int numberColleges = 0;
    private static long avgEarnings = 0;
    private static String state = "";
    private static String dependant = "";
    private static int degree = 0;
    private static double majorEarnings = 0;
    private static int majorCount = 0;
    private static double avgMajorEarnings = 0;
    private static TreeMap<Double, String> topScores = new TreeMap<>();

    private static Seq<Column> convertListToSeq(ArrayList<Column> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }

	public static void main(String[] args) {
        // Arguments: file locations, State, (In)Dependent, Degree index
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("College Scorecard"));
        SparkContext context = new SparkContext(sc.getConf());
        SparkSession sparkSession = new SparkSession(context);
        state = args[2];
        dependant = args[3];
        degree = Integer.parseInt(args[4]);
        Dataset<Row> db = sparkSession.read().csv(args[0]);
        JavaRDD<Row> earnings = db.select(col("INSTNM"),col("md_earn_wne_p10")).javaRDD();
        earnings.foreach(row -> {
            totalEarnings += row.getInt(1);
            numberColleges++;
        });
        avgEarnings = totalEarnings/numberColleges;
        JavaPairRDD<String, Double> collegeScore = earnings.mapToPair((row) -> {
            double score = avgEarnings/row.getInt(1);
            return new Tuple2<>(row.getString(0),score);
        });

        Dataset<Row> db2 = sparkSession.read().csv(args[1]);
        db2.createOrReplaceTempView("majors");
        JavaRDD<Row> majors = db2.filter("SELECT * FROM majors WHERE type =" + degree).javaRDD();

        majors.foreach(row -> {
            majorEarnings += row.getInt(1);
            majorCount++;
        });
        avgMajorEarnings = majorEarnings/majorCount;

        ArrayList<Column> col_majors = new ArrayList<>();
        ArrayList<Column> cols = new ArrayList<>();
        cols.add(col("INSTNM"));
        cols.add(col("STABBR"));
        cols.add(col("TUITIONFEE_IN"));
        cols.add(col("TUITIONFEE_OUT"));
        cols.add(col("DEP_DEBT_MDN"));
        cols.add(col("IND_DEBT_MDN"));
        switch(degree){
            case 1: cols.add(col("PCIP01"));
            case 2: cols.add(col("PCIP04"));
            case 3: cols.add(col("PCIP09"));
            case 4: cols.add(col("PCIP13"));
            case 5: cols.add(col("PCIP14"));
            case 6: cols.add(col("PCIP23"));
            case 7: cols.add(col("PCIP26"));
            case 8: cols.add(col("PCIP27"));
            case 9: cols.add(col("PCIP51"));
            case 10: cols.add(col("PCIP38"));
            case 11: cols.add(col("PCIP41"));
            case 12: cols.add(col("PCIP42"));
            case 13: cols.add(col("PCIP46"));
            case 14: cols.add(col("PCIP50"));
            case 15: cols.add(col("PCIP52"));
            case 16: cols.add(col("PCIP22"));
            case 17: cols.add(col("PCIP11"));
        }
        JavaRDD<Row> columns = db.select(convertListToSeq(cols)).javaRDD();
        JavaPairRDD<String, Double> scores = columns.mapToPair((row) -> {
            double debt;

            if (dependant.charAt(0) == 'D' || dependant.charAt(0) == 'd')
                debt = row.getDouble(4);
            else
                debt = row.getDouble(5);

            if (state.equalsIgnoreCase(row.getString(1)))
                debt -= row.getInt(3) - row.getInt(2);
            else
                debt += row.getInt(3) - row.getInt(2);

            double years = debt / (0.05 * avgMajorEarnings);

            return new Tuple2<>(row.getString(0), (1 - row.getFloat(6)) * years);
        });

        scores.foreach((college) -> {
            topScores.put(college._2, college._1);
            if (topScores.size() > 3)
                topScores.remove(topScores.lastKey());
        });
        JavaConverters.mapAsScalaMapConverter(topScores).asScala().toMap(Predef.conforms());

        sc.parallelize(new ArrayList<>(topScores.values())).coalesce(1).saveAsTextFile("/stuff");

        sparkSession.stop();
        sc.stop();
    }
}
