package org.example;

import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.impl.GraphImpl;
import org.apache.spark.graphx.impl.ReplicatedVertexView;
import scala.reflect.ClassTag;

public class partitionStrategy extends GraphImpl {
    public partitionStrategy(VertexRDD vertices,ReplicatedVertexView replicatedVertexView,ClassTag classTag$VD$0,ClassTag classTag$ED$0) {
        super(vertices, replicatedVertexView, classTag$VD$0, classTag$ED$0);
    }

    public partitionStrategy(ClassTag classTag$VD$0, ClassTag classTag$ED$0) {
        super(classTag$VD$0, classTag$ED$0);
    }

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

}
