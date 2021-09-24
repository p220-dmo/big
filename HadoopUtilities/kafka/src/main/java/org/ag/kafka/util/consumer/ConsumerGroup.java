package org.ag.kafka.util.consumer;

import org.ag.kafka.config.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Created by ahmed.gater on 13/11/2016.
 */
public class ConsumerGroup<K,V> {
    RecordHandler<K,V> handler ;
    ConsumerConfig conf  ;
    private List<CustomConsumer> consumers;
    private List<Thread> consumerThreads;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ConsumerGroup(ConsumerConfig consCfg, RecordHandler<K,V> handler) {
        this.conf = consCfg ;
        this.handler = handler ;
        consumers = new ArrayList();

        IntStream.range(0, Integer.valueOf(this.conf.getProps().get("consumer.number").toString())).forEach(
                i -> consumers.add(new CustomConsumer(this.conf,this.handler))
        ) ;
    }



    public void start() {
        consumerThreads = new ArrayList<>() ;
        consumers.stream().forEach(cust -> {Thread th = new Thread(cust) ; th.start();consumerThreads.add(th); }) ;
    }

    public void shutdown() {
       for(Thread th : this.consumerThreads){
            th.interrupt();
        }
        Thread.currentThread().interrupt();
    }

    public static void main(String[] args){
        ConsumerConfig consConf = new ConsumerConfig.ConsumerConfigBuilder("192.168.99.100:9092", "test").build() ;
        ConsumerGroup<String,String> consGrp = new ConsumerGroup<>(consConf, new BasicRecordHandler()) ;
        consGrp.start();
        try{
            Thread.sleep(1000);
            consGrp.shutdown();
        }catch(Exception e){

        }
    }

    private class CustomConsumer implements Runnable {
        ConsumerConfig conf  ;
        RecordHandler<K,V> handler ;
        KafkaConsumer<K,V> consumer ;

        public CustomConsumer(ConsumerConfig consCfg, RecordHandler<K,V> handler){
            this.conf = consCfg ;
            this.handler = handler ;
            consumer = new KafkaConsumer(conf.getProps());
            consumer.subscribe(Arrays.asList(conf.getTopics()));
        }

        public void run() {
            while (true) {
                ConsumerRecords records = consumer.poll(Integer.valueOf(conf.getProps().get("batch.size").toString()));
                this.handler.process(records) ;
                if (!Boolean.valueOf(conf.getProps().get("enable.auto.commit").toString())){
                    consumer.commitSync();
                }
            }
        }
        public void close(){
            consumer.close();
        }
    }
}
