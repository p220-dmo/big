package fr.htc.sales.kafka;

import java.util.Properties;

public class KafkaConfig {
    public static final String BROKERS = "sandbox-hdp.hortonworks.com:6667";
  

    
    public static Properties getProducerProps() {
    	
    	
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("acks", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "fr.htc.sales.kafka.SaleSerializer");
        return props;
    }

    public static Properties getConsumerProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BROKERS);
        props.setProperty("group.id", "testGroup");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("value.deserializer", "fr.htc.sales.kafka.SaleDeserializer");
        
        return props;
    }
}