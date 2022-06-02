package fr.htc.sales.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import fr.htc.sales.data.Sale;

public class KafkaSaleProducer {
	
	public static void sendMessages(Sale sale, String topicName) {
		KafkaProducer<Integer, Sale> producer = new KafkaProducer<Integer, Sale>(KafkaConfig.getProducerProps());
		
		System.out.printf("Sending message topic: %s, value: %s%n", topicName, sale);
		
		producer.send(new ProducerRecord<Integer, Sale>(topicName, sale.getCustomerId(), sale));
		
		producer.flush();
		producer.close();
	}
}