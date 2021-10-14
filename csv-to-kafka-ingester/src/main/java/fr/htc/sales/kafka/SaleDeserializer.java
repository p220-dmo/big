package fr.htc.sales.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import fr.htc.sales.data.Sale;

public class SaleDeserializer implements Deserializer<Sale> {
	private ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public Sale deserialize(String topic, byte[] data) {
		try {
			if (data == null) {
				System.out.println("Null received at deserializing");
				return null;
			}
			System.out.println("Deserializing...");
			return objectMapper.readValue(new String(data, "UTF-8"), Sale.class);
		} catch (Exception e) {
			throw new SerializationException("Error when deserializing byte[] to MessageDto");
		}
	}

}