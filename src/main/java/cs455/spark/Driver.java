package cs455.spark;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.*;

import static org.apache.spark.sql.functions.col;

public class Driver {

    private static String state = "";
    private static String dependant = "";
    private static int degree = 0;
    private static double majorEarnings = 0;
    private static double avgMajorEarnings = 0;
    private static TreeMap<Double, String> topScores = new TreeMap<>();

    public static void main(String[] args) {
        // Arguments: data, majors.csv, State, (In)Dependent, Degree index
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("College Scorecard")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        state = args[2];
        dependant = args[3];
        degree = Integer.parseInt(args[4]);

        Dataset<Row> db = sparkSession.read().format("csv").option("header", "true").load(args[1]);
        db.createOrReplaceTempView("majors");
        JavaRDD<Row> majors = db.filter(col("type").equalTo(degree)).javaRDD();

        majors.foreach(row -> {
            majorEarnings += Integer.parseInt(row.getString(3));
        });
        avgMajorEarnings = majorEarnings/majors.count();

        Dataset<Row> db2 = sparkSession.read().format("csv").option("header", true).load(args[0]);

        JavaPairRDD<Integer, String> colleges = db2.toJavaRDD().mapToPair((row -> {
           return new Tuple2<>(Integer.parseInt(row.getString(0)), row.toString().substring(row.toString().indexOf(',') + 1 ,
                   row.toString().length() - 1));
        }));

        JavaPairRDD<Integer, String> filter = colleges.mapToPair((college) -> {

            if (college._2.contains("PrivacySuppressed") || college._2.contains("NULL")) {
                return new Tuple2<>(college._1, "-1");
            }

            return new Tuple2<>(college._1, college._2);
        }).filter((college) -> college._2.length() > 2);

        /*
        0 - University name
        1 - State
        2 - Tuition In-state
        3 - Tuition Out-of-state
        4 - Dependent
        5 - Independent
        6 - Repayment Rate
        7 - PCIP01
        8 - PCIP04
        9 - PCIP09
        10 - PCIP13
        11 - PCIP14
        12 - PCIP23
        13 - PCIP26
        14 - PCIP27
        15 - PCIP51
        16 - PCIP38
        17 - PCIP41
        18 - PCIP42
        19 - PCIP46
        20 - PCIP50
        21 - PCIP52
        22 - PCIP22
        23 - PCIP11
         */
        JavaPairRDD<String, Double> scores = filter.mapToPair((row) -> {
            String[] values = row._2.split(",");
            double debt;
            try {
                if (dependant.charAt(0) == 'D' || dependant.charAt(0) == 'd')
                    debt = Double.parseDouble(values[4]);
                else
                    debt = Double.parseDouble(values[5]);
                if (state.equalsIgnoreCase(values[1]))
                    debt -= Double.parseDouble(values[3]) - Double.parseDouble(values[2]);
                else
                    debt += Double.parseDouble(values[3]) - Double.parseDouble(values[2]);
            } catch (Exception e) {
                debt = Integer.MAX_VALUE;
            }
            double years = debt / (0.05 * avgMajorEarnings);

            double val = Double.parseDouble(values[6 + degree]);

            double rate = Double.parseDouble(values[6]);

            return new Tuple2<>(row._1 + " " + values[0], (1 - rate) * val * years);
        });

        JavaPairRDD<String, Average> sums =
                scores.aggregateByKey(
                        (new Average(0, 0)),
                        (x, y) ->  (new Average(y, 1)),
                        (x, y) -> new Average(x.getSum() + y.getSum(), x.getNum() + y.getNum()));

        sums.foreach((college) -> {
            topScores.put(college._2.getAverage(), college._1);

        });
        for (Iterator<Double> iterator = topScores.descendingKeySet().iterator(); iterator.hasNext();) {
            Double val = iterator.next();
            if (topScores.size() > 20)
                iterator.remove();
        }

        JavaConverters.mapAsScalaMapConverter(topScores).asScala().toMap(Predef.conforms());
        sc.parallelize(new ArrayList<>(topScores.values())).coalesce(1).saveAsTextFile("/answer");

        sparkSession.stop();
        sc.stop();
    }
}
