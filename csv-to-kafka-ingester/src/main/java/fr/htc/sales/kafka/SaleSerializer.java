package fr.htc.sales.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import fr.htc.sales.data.Sale;

public class SaleSerializer implements Serializer<Sale> {
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public byte[] serialize(String topic, Sale data) {
		try {
			if (data == null) {
				System.out.println("Null received at serializing");
				return null;
			}
			System.out.println("Serializing...");
			return objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			throw new SerializationException("Error when serializing MessageDto to byte[]");
		}
	}
}