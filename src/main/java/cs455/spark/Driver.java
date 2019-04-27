package cs455.spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.SparkSession;

public class Driver {

    public Seq<Column> convertListToSeq(ArrayList<Column> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }

	public static void main(String[] args) throws Exception {
        // Arguements: file locations, State, (In)Dependent, Degree index
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("College Scorecard"));
        SparkContext context = new SparkContext(conf);
        SparkSession sparkSession = new SparkSession(context);
        String state = args[1];
        String dep = args[2];
        int degree = Integer.parseInt(args[3]);
        DataSet<Row> db = sparkSession.read().csv(args[0]);
        JavaRDD<Row> earnings = db.select(col("INSTNM"),col("md_earn_wne_p10")).javaRDD();
        long totalEarnings = 0;
        int numberColleges = 0;
        JavaPairRDD<String, Integer> studentEarnings = earning.mapToPairRDD<String,Integer>((row)->{
            String[] rows = row.split(",");
            numberColleges++;
            int value = Integer.parseInt(rows[1]);
            totalEarnings += value;
            return new Tuple2<String,Integer>(rows[0],value);
        }, this);
        long avgEarnings = totalEarnings/numberColleges;
        JavaPairRDD<String, Double> collegeScore = studentEarnings.mapToPairRDD((key,value)->{
            double score = totalEarnings/value;
            return new Tuple2<String,Integer>(key,score);
        });
        ArrayList<Column> cols = new ArrayList<Column>();
        cols.add(col("INSTNM"));
        cols.add(col("STABBR"));
        cols.add(col("ADM_RATE"));
        cols.add(col("COSTT4_A"));
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
        columns.flatMap((row) -> {
            String[] rows = row.split(",");
            
        });
        spark.stop();
        sc.stop();
    }
	
}
