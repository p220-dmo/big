package fr.htc.sales.job;

import fr.htc.sales.csv.OpenCsvExample;
import fr.htc.sales.kafka.KafkaSaleProducer;

public class Csv2KafkaLuncher {

	public static void main(String[] args) {
		if(args.length != 2) {
			System.out.println("Usage : java -jar csv-to-kafka-ingester.jar <filePath> <topicName> ");
			return;
		}
		String filePath = args[0];
		String topicName = args[1];
		
		
		OpenCsvExample.readCsvFileAsList(filePath)
			.forEach(sale -> KafkaSaleProducer.sendMessages(sale, topicName));
	}

}
