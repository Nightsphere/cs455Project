package cs455.spark;


import org.apache.avro.generic.GenericData;
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

    private static String state = "";
    private static String dependant = "";
    private static int degree = 0;
    private static double majorEarnings = 0;
    private static double avgMajorEarnings = 0;
    private static TreeMap<Double, String> topScores = new TreeMap<>();

    private static Seq<Column> convertListToSeq(ArrayList<Column> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }

    public static void main(String[] args) {
        // Arguments: file locations, State, (In)Dependent, Degree index
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("College Scorecard")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        state = args[2];
        dependant = args[3];
        degree = Integer.parseInt(args[4]);

        String schemaString = "INSTNM STABBR TUITIONFEE_IN TUITIONFEE_OUT DEP_DEBT_MDN IND_DEBT_MDN COMPL_RPY_7YR_RT ";

        Dataset<Row> db2 = sparkSession.read().format("csv").option("header", "true").load(args[1]);
        db2.createOrReplaceTempView("majors");
        JavaRDD<Row> majors = db2.filter(col("type").equalTo(degree)).javaRDD();

        majors.foreach(row -> {
            majorEarnings += Integer.parseInt(row.getString(3));
        });
        avgMajorEarnings = majorEarnings/majors.count();

        ArrayList<Column> cols = new ArrayList<>();
        cols.add(col("INSTNM").isNotNull());
        cols.add(col("STABBR").isNotNull());
        cols.add(col("TUITIONFEE_IN").isNotNull());
        cols.add(col("TUITIONFEE_OUT").isNotNull());
        cols.add(col("DEP_DEBT_MDN").isNotNull());
        cols.add(col("IND_DEBT_MDN").isNotNull());
        cols.add(col("COMPL_RPY_7YR_RT").isNotNull());

        switch(degree){
            case 1: cols.add(col("PCIP01").isNotNull());
                schemaString += "PCIP01"; break;
            case 2: cols.add(col("PCIP04").isNotNull());
                schemaString += "PCIP04"; break;
            case 3: cols.add(col("PCIP09").isNotNull());
                schemaString += "PCIP09"; break;
            case 4: cols.add(col("PCIP13").isNotNull());
                schemaString += "PCIP13"; break;
            case 5: cols.add(col("PCIP14").isNotNull());
                schemaString += "PCIP14"; break;
            case 6: cols.add(col("PCIP23").isNotNull());
                schemaString += "PCIP23"; break;
            case 7: cols.add(col("PCIP26").isNotNull());
                schemaString += "PCIP26"; break;
            case 8: cols.add(col("PCIP27").isNotNull());
                schemaString += "PCIP27"; break;
            case 9: cols.add(col("PCIP51").isNotNull());
                schemaString += "PCIP51"; break;
            case 10: cols.add(col("PCIP38").isNotNull());
                schemaString += "PCIP38"; break;
            case 11: cols.add(col("PCIP41").isNotNull());
                schemaString += "PCIP41"; break;
            case 12: cols.add(col("PCIP42").isNotNull());
                schemaString += "PCIP42"; break;
            case 13: cols.add(col("PCIP46").isNotNull());
                schemaString += "PCIP46"; break;
            case 14: cols.add(col("PCIP50").isNotNull());
                schemaString += "PCIP50"; break;
            case 15: cols.add(col("PCIP52").isNotNull());
                schemaString += "PCIP52"; break;
            case 16: cols.add(col("PCIP22").isNotNull());
                schemaString += "PCIP22"; break;
            case 17: cols.add(col("PCIP11").isNotNull());
                schemaString += "PCIP11"; break;
        }

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, false));
        }
        StructType schema = DataTypes.createStructType(fields);

//        Dataset<Row> db = sparkSession.read().format("csv").option("header", "true").option("inferSchema", "true").load(args[0]);
        Dataset<Row> db = sparkSession.read().csv();
        db.printSchema();

        Dataset<Row> column = db.select(convertListToSeq(cols));
        JavaRDD<Row> columns = column.javaRDD();
        columns.saveAsTextFile("/columns");

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
            try {
                val = Double.parseDouble(row.getString(7));
                rate = Double.parseDouble(row.getString(6));
            } catch (Exception e) {
                val = 0;
            }
            return new Tuple2<>(row.getString(0), (1 - rate) * (1 - val) * years);
        });

        scores.saveAsTextFile("/scores");

        scores.foreach((college) -> {
            if (college != null) {
                topScores.put(college._2, college._1);
                if (topScores.size() > 3)
                    topScores.remove(topScores.lastKey());
            }
        });

        JavaConverters.mapAsScalaMapConverter(topScores).asScala().toMap(Predef.conforms());

        sc.parallelize(new ArrayList<>(topScores.values())).coalesce(1).saveAsTextFile("/answer");

        sparkSession.stop();
        sc.stop();
    }
}
