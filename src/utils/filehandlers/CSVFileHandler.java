package utils.filehandlers;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
//import java.util.Iterator;
//import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CSVFileHandler {
	
	public static ArrayList<Map<String, String>> read(String path) throws IOException {
		Reader reader = new FileReader(path);
		Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(reader);
		ArrayList<Map<String, String>> recordsMap = new ArrayList<Map<String, String>>();
		for (CSVRecord record : records) {
			recordsMap.add(record.toMap());
		}
	return recordsMap;
	}
	
	public static boolean write(ArrayList<Map<String, String>> data, String destination) throws IOException {
		Writer writer = new FileWriter(destination);
		CSVFormat csvFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");
		CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);
		List<String> header = data.get(0).keySet().stream().collect(Collectors.toList());
		csvPrinter.printRecord(header);
		List<String> record;
		for (Map<String, String> datum : data) {
			record = datum.values().stream().collect(Collectors.toList());
			csvPrinter.printRecord(record);
		}
		writer.flush();
		writer.close();
		csvPrinter.close();
		return true;
	}
}
