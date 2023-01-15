package org.Paper;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class PPR {
    private static final Pattern SPACES = Pattern.compile("\\s+");
    private static final ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

    public static void main(String[] args) {
        checkArgs(args);

        // create sparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("PPR")
                .getOrCreate();

        // Loads all URLs from input file and initialize their neighbors.
//        JavaPairRDD<String, Iterable<String>> links = initSpark(spark, args[0]);
        Graph<String, String> graph = initGraph(spark,args[0]);
//        Partition[] partitions = graph.partitionBy(new newPS()).edges().getPartitions();

        List<Edge<Object>> e = graph.ops().pageRank(10, 0.15).edges().toJavaRDD().collect();
        System.err.println(e);

        System.out.println("OK");
        spark.close();
    }

    private static void checkArgs(String[] args){
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
            System.exit(1);
        }
    }

    private static Graph<String,String> initGraph(SparkSession sparkSession, String fileName){
        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        JavaRDD<String> lines = sparkSession.read().textFile(fileName).javaRDD();

        List<Edge<String>> edgeList = new ArrayList<>();

        // Loads all URLs from input file and initialize their neighbors.
//        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(s -> {
//            String[] parts = SPACES.split(s);
//            return new Tuple2<>(parts[0], parts[1]);
//        }).distinct().groupByKey().cache();

        JavaRDD<Edge<String>> edgesRDD = lines.map(s ->{
            //PaperCode/src/main/java/org/example/PPR.java pagerank_data.txt 10
            String[] parts = SPACES.split(s.trim());
            return new Edge<String>(Long.parseLong(parts[0]),Long.parseLong(parts[1]),"1");
        });
        Graph<String, String> graph = Graph.fromEdges(edgesRDD.rdd(), "",
                StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

        return graph;
    }
    private static class newPS implements PartitionStrategy {
        @Override
        public int getPartition(long src, long dst, int numParts) {
            //      val mixingPrime: VertexId = 1125899906842597L
            //      (math.abs(src * mixingPrime) % numParts).toInt
            Long mixingPrime = 1125899906842597L;
            return (int) Math.abs(src * mixingPrime) % numParts;
        }
    }
}
