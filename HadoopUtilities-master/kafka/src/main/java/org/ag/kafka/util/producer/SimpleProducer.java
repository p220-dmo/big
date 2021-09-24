package org.ag.kafka.util.producer;

import org.ag.kafka.config.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by ahmed.gater on 13/11/2016.
 */
/*
TODO: consider multi-threaded producer
 */
public class SimpleProducer<K,V> {
    ProducerConfig prodConfig ;
    Producer<K,V> producer ;

    public SimpleProducer(ProducerConfig prodCfg){
        this.prodConfig = prodCfg ;
        this.producer = new KafkaProducer<K,V>(prodConfig.getProps());
    }

    public void send(K k, V v){
        producer.send(new ProducerRecord<K, V>(prodConfig.getTopic(), k, v));
    }

    public void close(){
        producer.close();
    }
}
