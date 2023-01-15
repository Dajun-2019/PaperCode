package org.example;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 * <p>
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 * <p>
 * Example Usage:
 * <pre>
 * bin/run-example JavaPageRank data/mllib/pagerank_data.txt 10
 * </pre>
 * VM options中输入“-Dspark.master=local”
 */
public final class PageRank {
    private static final Pattern SPACES = Pattern.compile("\\s+");
    private  int  PROCESSOR_NUMS;


    public PageRank() {
    }

    public PageRank(int PROCESSOR_NUMS) {
        this.PROCESSOR_NUMS = PROCESSOR_NUMS;
    }

    static void showWarning() {
        String warning = "WARN: This is a naive implementation of PageRank " +
                "and is given as an example! \n" +
                "Please use the PageRank implementation found in " +
                "org.apache.spark.graphx.lib.PageRank for more conventional use.";
        System.err.println(warning);
    }

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }


    public static void main(String[] args) throws Exception {
        // JavaPageRank data/mllib/pagerank_data.txt 10
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
            System.exit(1);
        }

        showWarning();

        //build Session ，the name of session is JavaPageRank
        SparkSession spark = SparkSession
                .builder()
                .appName("PageRank")
                .getOrCreate();

        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
//        Graph<String,String> newGraph = Graph.fromEdges(lines.rdd());


        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
            String[] parts = SPACES.split(s);
            return new Tuple2<>(parts[0], parts[1]);
        }).distinct().groupByKey().cache();


        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < Integer.parseInt(args[1]); current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }

        // Collects all URL ranks and dump them to console.
        List<Tuple2<String, Double>> output = ranks.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
        }

        spark.stop();
    }
//    public Map<Integer,Integer> preSample(Graph<String,String> graph, int a){
//        long m = graph.ops().numEdges();
//        long n = graph.ops().numVertices();
//        long wp = m/n;
//
////        JavaPairRDD<String,String> wt = new JavaPairRDD<>();
//        Map<Edge<String>,Edge<String>> wt = new HashMap<>();
//
////        ClassTag<String> stringTaag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
//
//        for (int i = 0; i < PROCESSOR_NUMS; i++) {
//            Map<Edge<String>,Edge<String>> wa = new HashMap<>();
////            JavaPairRDD<String,String> waa = new JavaPairRDD<>(graph.edges(),String,String);
//            for (Edge<String> e: graph.edges().collect()) {
//                wa.put(e,e);
//            }
////            while(!wa.isEmpty()){
////                for (Iterator iterator: wa) {
////
////                }
////            }
////            Graph<String, String> rankGraph = graph.outerJoinVertices(graph.ops().outDegrees())
//        }
//
//        return new HashMap<>();
//    }



}