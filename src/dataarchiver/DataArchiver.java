package dataarchiver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import utils.filehandlers.CSVFileHandler;

public class DataArchiver {
	private String[] index = { "id", "ids" };
	private String[][] columnsOfInterest = {
			{"rsegid", "rsegids"},
			{"csegid", "csegids"},
	};
	private int numColumns;
	private String indexKey;
	private String indexKeyGetter;
	private String[] columnKeys;
	private String[] columnKeysGetter;
	private ArrayList<Map<String, String>> preCompressionArchive;
	private ArrayList<Map<String, String>> postCompressionArchive;
	
	public DataArchiver() {
		this.numColumns = this.columnsOfInterest.length;
		this.indexKey = this.index[0];
		this.columnKeys = new String[this.numColumns];
		for (int i=0; i<this.numColumns; i++) {
			this.columnKeys[i] = this.columnsOfInterest[i][1];
		}
		this.preCompressionArchive = new ArrayList<Map<String, String>>();
		this.postCompressionArchive = new ArrayList<Map<String, String>>();
	}
	
	public void archive(String[] requests) throws IOException {
		ArrayList<Map<String, String>> records;
		Set<String> header = null;
		boolean explode = false;
		Map<String, String> map = null;
		for (String request : requests) {
			System.out.println("Processing : " + request);
			records = CSVFileHandler.read(request);
			header = records.get(0).keySet();
			
			this.indexKeyGetter = null;
			// Populate indexKeyGetter
			// The schema supports list of indexes which means we should expolde the dataframe.
			// And then filter it by the columns of interest.
			if (header.contains(this.index[1])) {
				explode = true;
				this.indexKeyGetter = this.index[1];
			}
			// The schema has unique indexes. No need to explode.
			// Filtering the dataframe should be enough.
			else if (header.contains(this.index[0])) {
				explode = false;
				this.indexKeyGetter = this.index[0];
			} else {
				System.out.println("Cannot find the any index information!");
				System.out.println("What should be the index of the output format?");
			}
			
			this.columnKeysGetter = new String[this.numColumns];
			// Populate columnKeysGetter
			for (int i=0; i<this.numColumns; i++) {
				for (String column : this.columnsOfInterest[i]) {
					if (header.contains(column)) {
						this.columnKeysGetter[i] = column;
					}
				}
			}
			
			//System.out.println("indexKeyGetter : " + this.indexKeyGetter);
			//System.out.println("columnKeysGetter : " + Arrays.toString(this.columnKeysGetter));
			
			String[] indexes;
			// The immediate for loop needs a little refactoring.
			for (Map<String, String> record : records) {
				if (explode) {
					indexes = record.get(this.indexKeyGetter).split("\\|");
					for (String indexValue : indexes) {
						map = new HashMap<String, String>();
						map.put(this.indexKey, indexValue);
						for (int i=0; i<this.numColumns; i++) {
							map.put(this.columnKeys[i], record.get(this.columnKeysGetter[i]));
						}
						this.preCompressionArchive.add(map);
					}
				} else {
					map = new HashMap<String, String>();
					map.put(this.indexKey, record.get(this.indexKeyGetter));
					for (int i=0; i<this.numColumns; i++) {
						map.put(this.columnKeys[i], record.get(this.columnKeysGetter[i]));
					}
					this.preCompressionArchive.add(map);
				}
			}
		}
		// We now have singular indexes exploded records combined for all the requests.
		// Now is the time for some data processing.
		// Note that these are similar to dataframes except that these are a collection of HashMaps.
		System.out.println("Pre Compressed Archived Dataset");
		this.preCompressionArchive.forEach(System.out::println);
		
		// Get unique indexes from the whole dataset.
		Set<String> indexes = this.preCompressionArchive.stream()
				.map(x -> x.get(this.indexKey))
				.collect(Collectors.toSet());
		//indexes.forEach(System.out::println);
		
		// For every index in the dataset get unique values for each column of interest.
		List<String> columnsOfInterestValues;
		String[] listValues;
		String value;
		for (String index : indexes) {
			map = new HashMap<String, String>();
			map.put(this.indexKey, index);
			for (String columnKey : this.columnKeys) {
				columnsOfInterestValues = this.preCompressionArchive.stream()
						.filter(x -> x.get(this.indexKey).equals(index) && x.get(columnKey)!=null)
						.map(x -> x.get(columnKey))
						.collect(Collectors.toList());
				//columnsOfInterestValues.forEach(System.out::println);
				listValues = String.join("|", columnsOfInterestValues).split("\\|");
				listValues = Arrays.stream(listValues).distinct().toArray(String[]::new);
				value = String.join("|", listValues);
				map.put(columnKey, value);
			}
			this.postCompressionArchive.add(map);
		}
		//This needs to be dumped to HDFS.
		System.out.println("Post Compressed Archived Dataset");
		this.postCompressionArchive.forEach(System.out::println);
	}
	
	public boolean dump(String destination) throws IOException {
		System.out.println("Dumping the archive at : " + destination);
		return CSVFileHandler.write(this.postCompressionArchive, destination);
	}
	
	public void display() {
		System.out.println("IndexKey : " + this.indexKey);
		System.out.println("ColumnKeys : " + Arrays.toString(this.columnKeys));
	}
}
