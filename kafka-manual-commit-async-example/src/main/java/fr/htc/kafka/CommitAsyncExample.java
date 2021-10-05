package fr.htc.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.*;

public class CommitAsyncExample {
    private static String TOPIC_NAME = "_topic_test";
    private static KafkaConsumer<String, String> consumer;
    private static TopicPartition topicPartition;

    
    
    public static void main(String[] args) throws Exception {
        
    	int mode = -1;
    	if(args.length == 2) {
    		TOPIC_NAME = args[0];
    		mode = Integer.parseInt(args[1]);
    	}
    	
    	
    	Properties consumerProps = ExampleConfig.getConsumerProps();
        consumer = new KafkaConsumer<>(consumerProps);
        
        
        topicPartition = new TopicPartition(TOPIC_NAME, 0);
        consumer.assign(Collections.singleton(topicPartition));
        
        printOffsets("before consumer loop", consumer, topicPartition);
        
        switch (mode) {
    	case 1:
    		sendMessages();
    		break;
    	case 2:
    		startConsumer();
    		break;
    	case 3:
    		sendMessages();
    		startConsumer();
    		break;

    	default:
    		System.out.println("RAS");
    	}
        
        	
        
    }

    private static void startConsumer() {

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("consumed: key = %s, value = %s, partition id= %s, offset = %s%n", record.key(), record.value(), record.partition(), record.offset());
            }
            if (records.isEmpty()) {
                System.out.println("-- terminating consumer --");
                break;
            }
            printOffsets("before commitAsync() call", consumer, topicPartition);
            consumer.commitAsync();
            printOffsets("after commitAsync() call", consumer, topicPartition);
        }
        printOffsets("after consumer loop", consumer, topicPartition);
    }

    private static void printOffsets(String message, KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        Map<TopicPartition, OffsetAndMetadata> committed = consumer
                .committed(new HashSet<>(Arrays.asList(topicPartition)));
        OffsetAndMetadata offsetAndMetadata = committed.get(topicPartition);
        long position = consumer.position(topicPartition);
        System.out.printf("Offset info %s, Committed: %s, current position %s%n", message, offsetAndMetadata == null ? null : offsetAndMetadata.offset(), position);
    }

    private static void sendMessages() {
        Properties producerProps = ExampleConfig.getProducerProps();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);
        
        
        
        for (int i = 0; i < 10; i++) {
            String value = "message-" + i;
            System.out.printf("Sending message topic: %s, value: %s%n", TOPIC_NAME, value);
            
            producer.send(new ProducerRecord<>(TOPIC_NAME, value));
        }
        producer.flush();
        producer.close();
    }
}