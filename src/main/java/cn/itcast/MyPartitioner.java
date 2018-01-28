package cn.itcast;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * http://www.cnblogs.com/wxd0108/p/6519973.html
 */
public class MyPartitioner implements Partitioner {
    public MyPartitioner(VerifiableProperties properties) {
    }
    public int partition(Object key, int numPartitions) {
        return 2;
    }
}
