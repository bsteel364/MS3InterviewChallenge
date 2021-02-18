import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.sqlite.SQLiteException;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

public class DataLoader {

	private static CSVWriter csvWriter; // Used to write to purchases-bad.csv
	private static int recordsRecieved = 0;
	private static int recordsSucceeded = 0;
	private static int recordsFailed = 0;
	private static int batchSize = 35; // !!! Not sure how to optimize without knowing the ration ( good records / Bad records )
	private static boolean badQuery = false;

	public static void loadFromCsvToDatabase() {
		long start = System.nanoTime(); // Start Time
		initializeBadCsv(); // create purchases-bad.csv
		generateVerifiedRecordsFile();		
		boolean endOfFile = false;
		CSVReader csvReader;
		try {
			csvReader = new CSVReader(new FileReader("verified-records.csv"));
			String[] record;
			while ((record = csvReader.readNext()) != null) {
				badQuery = false;
				String[][] batch = new String[batchSize][];
				batch[0] = record; // Need to set the 1st before the loop because csvReader has no hasNext()
									// method, can only tell by reading the next
				for (int i = 1; i < batchSize; i++) {
					if ((record = csvReader.readNext()) != null) {
						batch[i] = record;
					} else {
						// THIS IS NOT TECHNICALLY A 3D LOOP - by the time this loop reaches execution,
						// the other loops will be done.
						for (int j = 0; j < i - 1; j++) { // This is to catch the last few records before it causes
							insertIntoSlow(batch[j]);
						}
						endOfFile = true;
						break;
					}
				}
				if (!endOfFile) { // If we are on the last (incomplete) batch, we cant use the batch method
					insertIntoBatch(batch);
				}
			}
			csvReader.close();
			csvWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long timeInMs = (System.nanoTime() - start) / 1000000;
		System.out.println("TIME TO PROCESS: " + timeInMs + " ms " + convertMsToMinutesAndSeconds(timeInMs));
		writeToLogFile(convertMsToMinutesAndSeconds(timeInMs));
	}

	public static void insertIntoBatch(String[][] batch) {
		Statement statement = null;
		try {
			statement = DatabaseConnectionDriver.connection.createStatement(); // From Driver class
			String query = "INSERT INTO purchases(A, B, C, D, E, F, G, H, I, J) VALUES(";
			int thisBatch = batchSize;
			for (int i = 0; i < batch.length; i++) {
				for (int j = 0; j < batch[i].length; j++) {
					if (j != batch[i].length - 1) {
						query += "'" + batch[i][j] + "', ";
					} else {
						query += "'" + batch[i][j] + "')";
					}
				}
				if (i != batch.length - 1) {
					query += ",(";
				}
			}
			try {
				statement.executeUpdate(query);
				System.out.println(query);
				recordsSucceeded += thisBatch;
			} catch (SQLiteException e) {
				// e.printStackTrace();
				badQuery = true;
				int i =0;
				while(badQuery) {
					insertIntoSlow(batch[i]);
					i++;
				}
				insertIntoBatch(batch, i);
			}
		} catch (SQLException e) {
			// e.printStackTrace();
		}
	}
	
	public static void insertIntoBatch(String[][] batch, int startIndex) {
		Statement statement = null;
		try {
			statement = DatabaseConnectionDriver.connection.createStatement(); // From Driver class
			String query = "INSERT INTO purchases(A, B, C, D, E, F, G, H, I, J) VALUES(";
			int thisBatch = batchSize - startIndex;
			for (int i = startIndex; i < batch.length; i++) {
				for (int j = 0; j < batch[i].length; j++) {
					if (j != batch[i].length - 1) {
						query += "'" + batch[i][j] + "', ";
					} else {
						query += "'" + batch[i][j] + "')";
					}
				}
				if (i != batch.length - 1) {
					query += ",(";
				}
			}
			try {
				statement.executeUpdate(query);
				System.out.println(query);
				recordsSucceeded += thisBatch;
			} catch (SQLiteException e) {
				// e.printStackTrace();
				badQuery = true;
				int i =startIndex;
				while(badQuery && i != batchSize) {
					insertIntoSlow(batch[i]);
					i++;
				}
				if(i != batchSize) {
					insertIntoBatch(batch, i);
				}
			}
		} catch (SQLException e) {
			// e.printStackTrace();
		}
	}

	// ***COULD get rid of this all together and make insertIntoBatch() perfectly recursive ?? -- would run into problems with bad records 
	// Create a single INSERT INTO query for each record and executes one at a time
	private static void insertIntoSlow(String[] record) {
		Statement statement = null;
		try {
			statement = DatabaseConnectionDriver.connection.createStatement(); // From Driver class
			String query = "INSERT INTO purchases(A, B, C, D, E, F, G, H, I, J) VALUES(";
			for (int i = 0; i < record.length; i++) {
				if (i != record.length - 1) {
					query += "'" + record[i] + "', ";
				} else {
					query += "'" + record[i] + "')";
				}
			}
			try {
				statement.executeUpdate(query);
				System.out.println("(slow) " + query);
				recordsSucceeded++;
			} catch (SQLiteException e) {
				System.out.println("BAD QUERY: " + query);
				writeBadQuery(record);
				badQuery = false;
			}
		} catch (SQLException e) {
			// e.printStackTrace();
		}
	}

	private static void initializeBadCsv() {
		try {
			csvWriter = new CSVWriter(new FileWriter(new File("purchases-bad.csv")));
			// String[] header = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J" }; DONT
			// KNOW IF THIS IS NECESSARY
			// csvWriter.writeNext(header);
		} catch (IOException e) {
			e.printStackTrace();
			// System.exit(0);
		}
	}

	private static void writeBadQuery(String[] badQuery) {
		csvWriter.writeNext(badQuery);
		recordsFailed++;
	}

	private static String convertMsToMinutesAndSeconds(long ms) {
		int seconds = (int) (ms / 1000);
		int minutes = seconds / 60;
		seconds = seconds % 60;
		return "(" + minutes + " mins, " + seconds + " sec)";
	}

	private static void writeToLogFile(String timeToProcess) {
		try {
			FileWriter logWriter = (new FileWriter(new File("purchases.log")));
			recordsRecieved = recordsSucceeded + recordsFailed;
			logWriter.write("Records Recieved: " + recordsRecieved + "\n");
			logWriter.write("Records Succeeded: " + recordsSucceeded + "\n");
			logWriter.write("Records Failed: " + recordsFailed + "\n");
			logWriter.write("Time to Process: " + timeToProcess);
			logWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static boolean verifyRecord(String[] record) {
		for (int i = 0; i < record.length; i++) {
			if (record[i].isEmpty()) {
				return false;
			}
		}
		return true;
	}

	public static void generateVerifiedRecordsFile() {
		initializeBadCsv();
		try {
			File verifiedRecords = new File("verified-records.csv");
			FileWriter fr = new FileWriter("verified-records.csv");
			CSVWriter recordWriter = new CSVWriter(fr);
			CSVReader csvReader = new CSVReader(new FileReader(new File("ms3Interview.csv")));
			Statement statement = DatabaseConnectionDriver.connection.createStatement(); // From Driver class
			String[] record; 
			while((record = csvReader.readNext()) != null) {
				if(verifyRecord(record)) {
					recordWriter.writeNext(record);
					fr.flush();
				}else {
					writeBadQuery(record);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
