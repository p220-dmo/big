package org.ag.kafka.config;

import lombok.Getter;

import java.util.Properties;

/**
 * Created by ahmed.gater on 13/11/2016.
 */
@Getter
public class ConsumerConfig {

    private String[] topics;
    private Properties props = new Properties();

    private ConsumerConfig(ConsumerConfigBuilder consumerCfgBuilder) {
        this.topics = consumerCfgBuilder.topics;
        this.props = consumerCfgBuilder.props;
    }

    public static class ConsumerConfigBuilder {
        private String[] topics;
        private Properties props = new Properties();

        public ConsumerConfigBuilder(String kafkaServerBootstrap, String... topics) {
            props = new Properties();
            props.put("bootstrap.servers", kafkaServerBootstrap);
            this.topics = topics;
            this.props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer ");
            this.props.put("batch.size", 100);
            this.props.put("enable.auto.commit", false);
            this.props.put("consumer.number",2) ;
        }


        public ConsumerConfigBuilder consumerNumber(int consumerNumber) {
            props.put("consumer.number", consumerNumber);
            return this;
        }

        public ConsumerConfigBuilder batchSize(int batchSize) {
            props.put("batch.size", String.valueOf(batchSize));
            return this;
        }

        public ConsumerConfigBuilder sessionTimeOut(int time) {
            props.put("session.timeout.ms", String.valueOf(time));
            return this;
        }

        public ConsumerConfigBuilder autoCommitInterval(int time) {
            props.put("auto.commit.interval.ms", String.valueOf(time));
            return this;
        }

        public ConsumerConfigBuilder autoCommit() {
            props.put("enable.auto.commit", "true");
            return this;
        }

        public ConsumerConfigBuilder gourpId(String groupId) {
            props.put("group.id", groupId);
            return this;
        }


        public ConsumerConfigBuilder keyDeserializer(String keySer) {
            this.props.put("key.deserializer", keySer);
            return this;
        }

        public ConsumerConfigBuilder valueDeserializer(String valSer) {
            this.props.put("value.deserializer", valSer);
            return this;
        }

        public ConsumerConfigBuilder prop(String K, String V) {
            this.props.put(K, V);
            return this;
        }

        public ConsumerConfig build() {
            return new ConsumerConfig(this);
        }
    }
}
