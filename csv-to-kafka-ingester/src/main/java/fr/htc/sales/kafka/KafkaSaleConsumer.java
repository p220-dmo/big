package fr.htc.sales.kafka;

import java.time.Duration;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import fr.htc.sales.data.Sale;

public class KafkaSaleConsumer {

	public static void consumeFilterAndSend(String topicNameSrc, String topicNameDest, Float caFilter) {

		KafkaConsumer<Integer, Sale> consumer = new KafkaConsumer<Integer, Sale>(KafkaConfig.getConsumerProps());
		consumer.subscribe(Collections.singletonList(topicNameSrc));

		ConsumerRecords<Integer, Sale> records = consumer.poll(Duration.ofSeconds(5));
		for (ConsumerRecord<Integer, Sale> record : records) {
			Sale sale = record.value();
			if (sale.getCa() > caFilter) {
				KafkaSaleProducer.sendMessages(sale, topicNameDest);
			}
			consumer.commitAsync();
		}

	}

}