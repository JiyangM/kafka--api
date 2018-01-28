package cn.itcast;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;


public class MyKafkaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list","mini1:9092");
        // 默认的序列化为byte改为string
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        /**
         * 自定义parition的基本步骤
         * 1、实现partition类
         * 2、加一个构造器，MyPartitioner(VerifiableProperties properties)
         * 3、将自定义的parititoner加入到properties中
         *    properties.put("partitioner.class","cn.itcast.MyPartitioner")
         * 4、producer.send方法中必须指定一个paritionKey
         */
        properties.put("partitioner.class","cn.itcast.MyPartitioner");
        Producer producer = new Producer(new ProducerConfig(properties));
        while (true){
            producer.send(new KeyedMessage("order4","zhang","我爱我的祖国"));
//            producer.send(new KeyedMessage("order","我爱我的祖国"));
        }
    }
}
