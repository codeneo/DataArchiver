package utils.filehandlers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class AvroFileHandler {
	
	public static ArrayList<Map<String, String>> read(String path) throws IOException {
		ArrayList<Map<String, String>> recordsMap = null;
		return recordsMap;
	}
	
	public static boolean write(ArrayList<Map<String, String>> data, String destination) throws IOException {
		return true;
	}
}
