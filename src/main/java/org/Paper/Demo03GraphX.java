package org.Paper;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.storage.StorageLevel;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;

public class Demo03GraphX {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("Graphx Learning").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

        //创建edges链表
        List<Edge<String>> edges = new ArrayList<>();
        //向其中加边
        edges.add(new Edge<String>(1L, 2L, "Friend1"));
        edges.add(new Edge<String>(1L, 4L, "Friend2"));
        edges.add(new Edge<String>(2L, 4L, "Friend3"));
        edges.add(new Edge<String>(3L, 1L, "Friend4"));
        edges.add(new Edge<String>(3L, 4L, "Friend5"));

        JavaRDD<Edge<String>> edgesRDD = sc.parallelize(edges);

        
        Graph<String, String> graph = Graph.fromEdges(edgesRDD.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

//        Graph<String, String> graph = Graph.fromEdges(System.out.println("OK"), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);



        //Scala Trait(特征) 相当于 Java 的接口，实际上它比接口还功能强大。与接口不同的是，它还可以定义属性和方法的实现。
        //一般情况下Scala的类只能够继承单一父类，但是如果是 Trait(特征) 的话就可以继承多个，从结果来看就是实现了多重继承。
        PartitionStrategy newPS = new PartitionStrategy() {
            @Override
            public int getPartition(long src, long dst, int numParts) {
                //      val mixingPrime: VertexId = 1125899906842597L
                //      (math.abs(src * mixingPrime) % numParts).toInt
                Long mixingPrime = 1125899906842597L;
                return (int) Math.abs(src * mixingPrime) % numParts;
            }
        };
        graph.partitionBy(newPS);


        System.out.println(graph.edges().filter((edge) -> edge.srcId() > edge.dstId()
//            if (edge.attr == "Friend1") {
//                return true;
//            }
//            return false;
        ));

        //所有关于graph的operators都在graph.ops()对象中
//        System.err.println(graph.ops().degrees());
//        graph.partitionBy(PartitionStrategy.fromString())

        List<Edge<String>> e = graph.edges().toJavaRDD().collect();
//        System.out.println(e);

    }

}