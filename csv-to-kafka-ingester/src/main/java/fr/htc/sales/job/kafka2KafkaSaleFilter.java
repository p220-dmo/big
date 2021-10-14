package fr.htc.sales.job;

import fr.htc.sales.kafka.KafkaSaleConsumer;

public class kafka2KafkaSaleFilter {
	
	
	public static void main(String[] args) {
		
		if(args.length != 3) {
			System.out.println("Usage : java -jar kafka-to-kafka-filter.jar <topicNameSrc>  <topicNameDest> <ca> ");
			return;
		}
		String topicNameSrc = args[0];
		String topicNameDest = args[1];
		Float CAFilter = Float.parseFloat(args[2]);
		
		KafkaSaleConsumer.consumeFilterAndSend(topicNameSrc, topicNameDest, CAFilter);
	}

}
