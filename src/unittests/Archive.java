package unittests;

import java.io.IOException;

import dataarchiver.DataArchiver;

public class Archive {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		DataArchiver dataArchiver = new DataArchiver();
		//dataArchiver.display();
		String[] requests = {"PATH/TO/FILE_ONE.CSV", "PATH/TO/FILE_TWO.CSV"};
		dataArchiver.archive(requests);
		
		String destination = "PATH/TO/FILE_DESTINATION.CSV";
		dataArchiver.dump(destination);
		
			
	}

}
