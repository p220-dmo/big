package org.ag.kafka.config;

import lombok.Getter;

import java.util.Properties;

/**
 * Created by ahmed.gater on 13/11/2016.
 */
@Getter
public class AdminConfig {
    private String zkConnectionString ;
    private String kafkaServerBootstrap ;
    private int sessionTimeoutMs = 10 * 1000;
    private int connectionTimeoutMs = 8 * 1000;
    private boolean isSecureKafkaCluster = false;
    private Properties props = new Properties();

    private AdminConfig(KafkaConfigBuilder kafkaCfgBuilder){
        this.kafkaServerBootstrap = kafkaCfgBuilder.kafkaServerBootstrap ;
        this.zkConnectionString = kafkaCfgBuilder.zkConnectionString ;
        this.isSecureKafkaCluster = kafkaCfgBuilder.isSecureKafkaCluster ;
        this.sessionTimeoutMs = kafkaCfgBuilder.sessionTimeoutMs ;
        this.connectionTimeoutMs = kafkaCfgBuilder.connectionTimeoutMs ;
        this.props = kafkaCfgBuilder.props ;
    }

    public static class KafkaConfigBuilder {
        private String zkConnectionString ;
        private String kafkaServerBootstrap ;
        private Properties props ;
        private boolean isSecureKafkaCluster = false;
        private int sessionTimeoutMs = 10 * 1000;
        private int connectionTimeoutMs = 8 * 1000;

        public KafkaConfigBuilder(String kafkaServerBootstrap,String zkConnectionString) {
            this.kafkaServerBootstrap = kafkaServerBootstrap;
            this.zkConnectionString = zkConnectionString ;
            props = new Properties() ;
            props.put("bootstrap.servers",this.kafkaServerBootstrap) ;
        }

        public KafkaConfigBuilder(String kafkaServerBootstrap,String zkConnectionString,boolean isSecured) {
            this(kafkaServerBootstrap,zkConnectionString) ;
            this.isSecureKafkaCluster = isSecured ;
        }

        public KafkaConfigBuilder prop(String K, String V) {
            this.props.put(K,V) ;
            return this;
        }

        public KafkaConfigBuilder sessionTimeOut(int sessionTimeOut) {
            this.sessionTimeoutMs = sessionTimeOut  ;
            return this;
        }

        public KafkaConfigBuilder connectionTimeOut(int connctionTimeOut) {
            this.connectionTimeoutMs = connctionTimeOut  ;
            return this;
        }

        public AdminConfig build() {
            return new AdminConfig(this);
        }
    }
}
