package org.ag.kafka.util.consumer;

import org.ag.kafka.config.ConsumerConfig;
import org.junit.Test;

/**
 * Created by ahmed.gater on 15/11/2016.
 */
public class ConsumerGroupTest {
    ConsumerConfig consConf = new ConsumerConfig.ConsumerConfigBuilder("192.168.99.100:9092", "test").build() ;
    ConsumerGroup<String,String> consGrp = new ConsumerGroup<>(consConf, new BasicRecordHandler()) ;

    @Test
    public void startTest() throws Exception {
        consGrp.start();
        Thread.sleep(10000);
        System.out.println("Salam Alikoum shutdown") ;
        consGrp.shutdown();
    }

}