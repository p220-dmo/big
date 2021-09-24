package org.ag.kafka.config;

import lombok.Getter;

import java.util.Properties;

/**
 * Created by ahmed.gater on 13/11/2016.
 */
@Getter
public class ProducerConfig {

    String topic ;
    Properties props = new Properties() ;
    private ProducerConfig(ProducerConfigBuilder producerCfgBuilder){
        this.topic = producerCfgBuilder.topic ;
        this.props = producerCfgBuilder.props ;
    }

    public static class ProducerConfigBuilder {
        String topic ;
        Properties props ;

        public ProducerConfigBuilder(String kafkaServerBootstrap, String topic) {
            props = new Properties() ;
            props.put("bootstrap.servers",kafkaServerBootstrap) ;
            this.props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            this.props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            this.topic = topic ;
        }

        public ProducerConfigBuilder prop(String K, String V) {
            this.props.put(K,V) ;
            return this;
        }

        public ProducerConfigBuilder acks(String acks) {
            this.props.put("acks", acks);
            return this;
        }

        public ProducerConfigBuilder retries(int r){
            this.props.put("retries", r);
            return this ;
        }

        public ProducerConfigBuilder batchSize(long l){
            this.props.put("batch.size", l);
            return this;
        }

        public ProducerConfigBuilder bufferMemory(long l){
            this.props.put("buffer.memory", l);
            return this;
        }

        public ProducerConfigBuilder keySerializer(String keySer){
            this.props.put("key.serializer", keySer);
            return this ;
        }

        public ProducerConfigBuilder valueSerializer(String valSer){
            this.props.put("value.serializer", valSer) ;
            return this;
        }

        public ProducerConfig build() {
            return new ProducerConfig(this);
        }
    }
}
