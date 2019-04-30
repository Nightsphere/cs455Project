package cs455.spark;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.apache.spark.sql.functions.col;

public class Driver {

	private static String major;
    private static String state = "";
    private static String dependant = "";
    private static int degree = 0;
    private static double majorEarnings = 0;
    private static double avgMajorEarnings = 0;
    private static TreeMap<Double, String> topScores = new TreeMap<>();
    private static int instnm = 0;
    private static int stabbr = 0;
    private static int instate = 0;
    private static int outstate = 0;
    private static int dep = 0;
    private static int ind = 0;
    private static int rate = 0;
    private static int pci = 0;

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

        Dataset<Row> db = sparkSession.read().format("csv").load(args[0]);
        Dataset<Row> db2 = sparkSession.read().format("csv").option("header", "true").load(args[1]);
        db2.createOrReplaceTempView("majors");
        JavaRDD<Row> majors = db2.filter(col("type").equalTo(degree)).javaRDD();

        majors.foreach(row -> {
            majorEarnings += Integer.parseInt(row.getString(3));
        });
        avgMajorEarnings = majorEarnings/majors.count();
        
        major = "";
        
        switch(degree){
            case 1: major = "PCIP01"; break;
            case 2: major = "PCIP04"; break;
            case 3: major = "PCIP09"; break;
            case 4: major = "PCIP13"; break;
            case 5: major = "PCIP14"; break;
            case 6: major = "PCIP23"; break;
            case 7: major = "PCIP26"; break;
            case 8: major = "PCIP27"; break;
            case 9: major = "PCIP51"; break;
            case 10: major = "PCIP38"; break;
            case 11: major = "PCIP41"; break;
            case 12: major = "PCIP42"; break;
            case 13: major = "PCIP46"; break;
            case 14: major = "PCIP50"; break;
            case 15: major = "PCIP52"; break;
            case 16: major = "PCIP22"; break;
            case 17: major = "PCIP11"; break;
        }
        
        JavaRDD<String> colleges = db.toJavaRDD().map(row -> {
        	String result = row.getString(3) +", "+ row.getString(5)+", "+row.getString(378)+", "+row.getString(379)+", "+row.getString(1509)
        	+row.getString(1510)+", "+row.getString(1394)+", "+row.getString(61)+", "+row.getString(63) +", "+ row.getString(65)
        	+", "+row.getString(69)+", "+row.getString(70)+", "+row.getString(75)+", "+row.getString(78)+", "+row.getString(79)
        	+", "+row.getString(96)+", "+row.getString(83)+", "+row.getString(86)+", "+row.getString(87)+", "+row.getString(91)
        	+", "+row.getString(95)+", "+row.getString(97)+", "+row.getString(74)+", "+row.getString(67);
        	return result;
        });
        colleges.saveAsTextFile("/Users/cblack/Desktop/cs455Project/cleaned");

        /*Dataset<Row> column = db.select(convertListToSeq(cols));
        JavaRDD<Row> columns = column.javaRDD();

        JavaPairRDD<String, Double> scores = columns.mapToPair((row) -> {
            double debt;

            try {
                if (dependant.charAt(0) == 'D' || dependant.charAt(0) == 'd')
                    debt = Double.parseDouble(row.getString(4));
                else
                    debt = Double.parseDouble(row.getString(5));

                if (state.equalsIgnoreCase(row.getString(1)))
                    debt -= Double.parseDouble(row.getString(3)) - Double.parseDouble(row.getString(2));
                else
                    debt += Double.parseDouble(row.getString(3)) - Double.parseDouble(row.getString(2));
            } catch (Exception e) {
                debt = Integer.MAX_VALUE;
            }

            double years = debt / (0.05 * avgMajorEarnings);

            double rate = 0;
            double val = 0;
            System.out.println("Double at index 7: "+Double.parseDouble(row.getString(7)));
            try {
                val = Double.parseDouble(row.getString(7));
               
                rate = Double.parseDouble(row.getString(6));
            } catch (Exception e) {
                val = 0;
            }
            return new Tuple2<>(row.getString(0), (1 - rate) * (1 - val) * years);
        });

        scores.saveAsTextFile("/Users/cblack/Desktop/cs455Project/scores");

        scores.foreach((college) -> {
            if (college != null) {
                topScores.put(college._2, college._1);
                if (topScores.size() > 3)
                    topScores.remove(topScores.lastKey());
            }
        });

        JavaConverters.mapAsScalaMapConverter(topScores).asScala().toMap(Predef.conforms());

        sc.parallelize(new ArrayList<>(topScores.values())).coalesce(1).saveAsTextFile("/answer");*/

        sparkSession.stop();
        sc.stop();
    }
}
