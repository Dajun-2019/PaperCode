package org.GraphX;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;

public class Demo03GraphX {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Graphx Learning").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

        List<Edge<String>> edges = new ArrayList<>();
        edges.add(new Edge<String>(1L, 2L, "Friend1"));
        edges.add(new Edge<String>(1L, 4L, "Friend2"));
        edges.add(new Edge<String>(2L, 4L, "Friend3"));
        edges.add(new Edge<String>(3L, 1L, "Friend4"));
        edges.add(new Edge<String>(3L, 4L, "Friend5"));

        JavaRDD<Edge<String>> edgesRDD = sc.parallelize(edges);

        Graph<String, String> graph = Graph.fromEdges(edgesRDD.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

        List<Edge<String>> e = graph.edges().toJavaRDD().collect();
        System.out.println(e);

    }

}