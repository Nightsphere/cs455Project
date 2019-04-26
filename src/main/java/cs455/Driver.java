package cs455;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class Driver {
    private static final Pattern CSPACE = Pattern.compile(": ");

    @SuppressWarnings("serial")
    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <linksFile> <titlesFile> <number_of_iterations>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaPageRank")
                .master("local") //spark://harrisburg.cs.colostate.edu:30207")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> names = spark.read().textFile(args[1]).javaRDD();


        JavaPairRDD<String, Long> nameList = names.zipWithIndex();
        JavaPairRDD<String, Long> swappedNameList = nameList.mapToPair(te -> {
            return new Tuple2<>(te._1, te._2 + 1);
        });
        JavaPairRDD<String, String> trueNameList = swappedNameList.mapToPair(st -> {
            return new Tuple2<>(Long.toString(st._2), st._1);
        });


        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<String, List<String>> links = lines.mapToPair(s -> {
            String[] parts = CSPACE.split(s);
            return new Tuple2<>(parts[0], Arrays.asList(parts[1].split(" ")));
        }).cache(); //.distinct().groupByKey().cache()

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);
        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < Integer.parseInt(args[3]); current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1);
                        List<Tuple2<String, Double>> results = new ArrayList<>();

                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    }).cache();

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }

        // Create the dataset for the ranks RDD. Written as STRING index, Dow toOUBLE page rank
        Dataset<Tuple2<String, Double>> ranksDS =  spark.createDataset(ranks.collect(),
                Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE())).cache();
        ranksDS.createOrReplaceTempView("RANK_TABLE");

        // Create the dataset for the titles RDD. Written as STRING line number, STRING article title
        Dataset<Tuple2<String, String>> titlesDS =  spark.createDataset(trueNameList.collect(),
                Encoders.tuple(Encoders.STRING(), Encoders.STRING())).cache();
        titlesDS.createOrReplaceTempView("TITLE_TABLE");


        Dataset<Row> ranksSet = spark.sql("SELECT _1 FROM RANK_TABLE ORDER BY _2 DESC LIMIT 10");
        Dataset<Row> set = spark.sql("SELECT _2 FROM RANK_TABLE ORDER BY _2 DESC LIMIT 10");

        //Dataset<Row> test = spark
        //      .sql("SELECT RANK_TABLE._1, TITLE_TABLE._2 FROM RANK_TABLE JOIN TITLE_TABLE ON RANK_TABLE._1=TITLE_TABLE._1");
        //Dataset<Row> titleSet = spark
        //      .sql("SELECT _1, _2 FROM TITLE_TABLE WHERE _1 IN (SELECT _1 FROM RANK_TABLE ORDER BY _2 DESC LIMIT 10)");

        //JavaRDD<Row> answer = titleSet.rdd().toJavaRDD().coalesce(1);

        set.show();
        ranksSet.rdd().toJavaRDD().coalesce(1).saveAsTextFile(args[2]);

        spark.stop();
    }
}