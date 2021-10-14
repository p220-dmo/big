package fr.htc.sales.csv;

import java.io.FileReader;
import java.util.List;
import java.util.stream.Collectors;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import fr.htc.sales.data.Sale;

public class OpenCsvExample {

	public static List<Sale> readCsvFileAsList(String path) {

		try {

			CSVParser parser = new CSVParserBuilder().withSeparator(';').build();

			CSVReader csvReader = new CSVReaderBuilder(new FileReader(path)).withCSVParser(parser).build();
			
			return csvReader.readAll().stream()
					.map(tokens -> Sale.parseLine(tokens))
						.collect(Collectors.toList());

		} catch (Exception e) {
			System.out.println("Error parsing csv file");
			return null;
		}
	}

}