package master2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 *
 * @authors Alvaro Feal; Gorka Revilla
 * @version 0.1
 * @since 23-01-2017
 */
public class HbaseApp {

	// GroupID
	private static final String GROUP_ID = "16";

	// Prints to be displayed
	private static final boolean DEBUG = false;
	private static final boolean INFO = true;

	// Parameters for the program
	private final static String TABLENAME = "twitterStats";
	private final static int MAXVERSIONS = 100;
	private final static int KEYSIZE = 42; // language 2, hashtag 40.

	// Parameters
	private static int mode;
	private static String zkHost;
	private static long startTS;
	private static long endTS;
	private static int topN;
	private static String[] languages;
	private static String dataFolder;
	private static String outputFolder;

	// Name of tables and families
	private static byte[] tableTwitter = Bytes.toBytes(TABLENAME);
	private static byte[] familyLang = Bytes.toBytes("lang"); // CF_1
	private static byte[] familyHt = Bytes.toBytes("ht"); // CF_2

	// Columns
	private static byte[] columnName = Bytes.toBytes("name");
	private static byte[] columnFreq = Bytes.toBytes("freq");

	// Hbase Objects
	private static HTableDescriptor tableDescriptor;
	private static HTable hTableTwitter;
	private static Configuration hConfiguration;
	private static HBaseAdmin hAdmin;

	// Data Structures
	private static Map<String, Integer> hashtagList;

	/**
	 * 
	 * @param args
	 *            should be 8: 0-Mode: - 1: - 2: - 3: - 4: 1-zkHost: 2-startTS:
	 *            3-endTS: 4-N: 5-Languages: 6-dataFolder: 7-outputFolder:
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			printUsageAndExit();
		}

		// Load parameters
		mode = Integer.valueOf(args[0]);

		switch (mode) {
		case 1:
			zkHost = args[1];
			startTS = Long.valueOf(args[2]);
			endTS = Long.valueOf(args[3]);
			topN = Integer.valueOf(args[4]);
			languages = args[5].split(",");
			outputFolder = args[6];
			break;
		case 2:
			zkHost = args[1];
			startTS = Long.valueOf(args[2]);
			endTS = Long.valueOf(args[3]);
			topN = Integer.valueOf(args[4]);
			languages = args[5].split(",");
			outputFolder = args[6];
			break;
		case 3:
			zkHost = args[1];
			startTS = Long.valueOf(args[2]);
			endTS = Long.valueOf(args[3]);
			topN = Integer.valueOf(args[4]);
			outputFolder = args[5];
			break;

		case 4:
			zkHost = args[1];
			dataFolder = args[2];
			break;

		default:
			printUsageAndExit();
			break;
		}

		if (DEBUG) {
			System.out.println("Parameters: " + mode + ", " + zkHost + ", " + startTS + ", " + endTS + ", " + topN
					+ ", " + Arrays.toString(languages) + ", " + dataFolder + ", " + outputFolder);
		}

		boolean flag_ts_correct = endTS > startTS;

		// Connect to de HBase
		connectHBase();

		// DO
		if (mode == 1 && flag_ts_correct) {
			runQuery1();
		} else if (mode == 2 && flag_ts_correct) {
			runQuery2();
		} else if (mode == 3 && flag_ts_correct) {
			runQuery3();
		} else if (mode == 4) {
			createDDBB();
		} else {
			printUsageAndExit();
		}

	}

	/**
	 * Method to make all the connections to the HBase DDBB
	 */
	private static void connectHBase() {

		String[] host = zkHost.split(":");

		try {
			hConfiguration = HBaseConfiguration.create();
			hConfiguration.set("hbase.zookeeper.quorum", host[0]);
			hConfiguration.setInt("hbase.zookeeper.property.clientPort", Integer.valueOf(host[1]));

			hAdmin = new HBaseAdmin(hConfiguration);
			hTableTwitter = new HTable(hConfiguration, TABLENAME);
		} catch (NumberFormatException e) {
			System.err.println("Error, zkHost parameter is not properly written, check it.");
			System.exit(-1);
		} catch (MasterNotRunningException e) {
			System.err.println("Error, The Master is not running.");
		} catch (ZooKeeperConnectionException e) {
			System.err.println("Error, Can not connect to the Zookeper. Check the connection.");
		} catch (IOException e) {
			System.err.println("Error, Can not writte or read in the HBase.");
		}

	}

	/**
	 * Given a language (lang), find the Top-N most used words for the given
	 * language in a time interval defined with a start and end timestamp. Start
	 * and end timestamp are in milliseconds
	 * 
	 * @param languages
	 */
	public static void runQuery1() {
		if (languages.length != 1) {
			printQuery1Usage();
		}

		runLanguageQuery(languages[0]);

	}

	/**
	 * Find the list of Top-N most used words for each language in a time
	 * interval defined with the provided start and end timestamp. Start and end
	 * timestamp are in milliseconds.
	 */
	public static void runQuery2() {
		if (languages.length < 1) {
			printQuery2Usage();
		}

		for (String lang : languages) {
			runLanguageQuery(lang);

		}
	}

	/**
	 * Find the Top-N most used words and the frequency of each word regardless
	 * the language in a time interval defined with the provided start and end
	 * timestamp. Start and end timestamp are in milliseconds.
	 */
	public static void runQuery3() {
		if (languages != null) {
			printQuery3Usage();
		}

		runLanguageQuery(null);
	}

	/**
	 * Method to run all the diferent querys depending on the parameter lang. If
	 * it is not null, will check the values for that language, if it is null,
	 * for any language.
	 * 
	 * @param lang
	 */
	private static void runLanguageQuery(String lang) {
		try {

			// Instantiating the Scan class
			Scan scan = new Scan();

			if (lang != null) {
				Filter f = new SingleColumnValueFilter(familyLang, columnName, CompareFilter.CompareOp.EQUAL,
						Bytes.toBytes(lang));
				scan.setFilter(f);
			}

			// Scanning the required columns
			scan.addColumn(familyLang, columnName);
			scan.addColumn(familyHt, columnName);
			scan.addColumn(familyHt, columnFreq);
			scan.setTimeRange(startTS, endTS);
			scan.setMaxVersions(MAXVERSIONS);

			// Getting the scan result
			ResultScanner scanner = hTableTwitter.getScanner(scan);

			hashtagList = new HashMap<String, Integer>();
			ArrayList<String> hts = new ArrayList<String>();
			ArrayList<Integer> fs = new ArrayList<Integer>();
			// Reading values from scan result
			for (Result result = scanner.next(); result != null; result = scanner.next()) {

				// Reading Cells for each result
				for (Cell cell : result.getColumnCells(familyHt, columnName)) {
					hts.add(Bytes.toString(CellUtil.cloneValue(cell)));

				}
				for (Cell cell : result.getColumnCells(familyHt, columnFreq)) {
					fs.add(Integer.valueOf(new String(CellUtil.cloneValue(cell))));
				}
				for (int i = 0; i < hts.size(); i++) {
					addToList(hts.get(i), fs.get(i));
				}
				hts.clear();
				fs.clear();
			}

			// closing the scanner
			scanner.close();

			if (INFO)
				if (lang != null)
					System.out.println("Top" + topN + " of the language: " + lang);
				else
					System.out.println("Top" + topN + ":");

			String[][] TopN = getTopNArray(topN);

			// if (DEBUG)
			// System.out.println(Arrays.deepToString(TopN));

			writeFile(lang, TopN);

		} catch (TableNotFoundException e) {
			System.err.println("Error, Table is not created, try with mode 4.");

		} catch (IOException e) {
			System.err.println("Error, Can not read from the database");
			// e.printStackTrace();
		}
	}

	/**
	 * Create and load data from the files. File names have the format
	 * “lang.out”, for example en.out, it.out, es.out
	 */
	public static void createDDBB() {

		if (zkHost == null || dataFolder == null) {
			printCreateDDBBUsage();
		}

		try {

			// First we clean the data
			// If is the first time, it will throw a TableNotFoundException
			cleanTable(hAdmin);

			// And now we add the tables and families

			tableDescriptor = new HTableDescriptor(TableName.valueOf(tableTwitter));
			HColumnDescriptor family_1 = new HColumnDescriptor(familyLang);
			HColumnDescriptor family_2 = new HColumnDescriptor(familyHt);
			family_1.setMaxVersions(MAXVERSIONS);
			family_2.setMaxVersions(MAXVERSIONS);
			tableDescriptor.addFamily(family_1);
			tableDescriptor.addFamily(family_2);
			hAdmin.createTable(tableDescriptor);

			if (DEBUG)
				System.out.println("Database Created.");
			// Finally we read from the file and add the values

			File folder = new File(dataFolder);
			File[] listOfFiles = folder.listFiles();

			for (File file : listOfFiles) {
				if (file.isFile() && file.getName().substring(file.getName().lastIndexOf('.')).equals(".out")) {
					FileInputStream fstream = new FileInputStream(dataFolder + "/" + file.getName());
					BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

					String strLine;

					// Read File Line By Line
					while ((strLine = br.readLine()) != null) {
						String[] values = strLine.split(",");
						addValues(values[0], values[1], values[2], values[3]);
						addValues(values[0], values[1], values[4], values[5]);
						addValues(values[0], values[1], values[6], values[7]);
					}

					// Close the input stream
					br.close();
				}
			}

		} catch (ZooKeeperConnectionException ex) {
			System.out.println("Error Connecting with the HBase.");
			printUsageAndExit();
		} catch (IOException ex) {
			System.out.println("Error Writting or reading in HBase.");
			printUsageAndExit();
		}
	}

	/**
	 * Add hashtag to list or increase the freq value
	 * 
	 * @param hashtag
	 * 
	 */
	private static void addToList(String hashtag, int freq) {

		if (DEBUG)
			System.out.println("Adding: " + hashtag + "," + freq);

		// If contains the hashtag, add freq
		if (hashtagList.containsKey(hashtag)) {
			hashtagList.put(hashtag, hashtagList.get(hashtag) + freq);
		} else { // Add with freq
			hashtagList.put(hashtag, freq);
		}

	}

	/**
	 * Get the Top N array with the most used hashtags and the frecuency.
	 * 
	 * @return array N rows and 2 cols with the topN Top1 = array[0][x] Top2 =
	 *         array[1][x] Top3 = array[2][x] ...
	 */
	private static String[][] getTopNArray(int n) {
		String[][] topN = new String[n][2];

		ValueComparator bvc = new ValueComparator(hashtagList);
		TreeMap<String, Integer> sorted_hashtag = new TreeMap<String, Integer>(bvc);

		sorted_hashtag.putAll(hashtagList);
		Set<Entry<String, Integer>> set = sorted_hashtag.entrySet();
		Iterator<Entry<String, Integer>> i = set.iterator();
		for (int c = 0; c < n; c++) {

			if (i.hasNext() == false) {
				topN[c][0] = "null";
				topN[c][1] = "0";

			} else {
				Map.Entry<String, Integer> me = (Map.Entry<String, Integer>) i.next();
				topN[c][0] = me.getKey().toString();
				topN[c][1] = me.getValue().toString();
			}
		}

		if (INFO) {
			for (int c = 0; c < n; ++c) {
				System.out.println("=> Top" + (c + 1) + ": " + topN[c][0] + ":" + topN[c][1]);
			}
		}

		return topN;
	}

	/**
	 * 
	 * Insert values in the HBase
	 * 
	 * @param timestamp
	 * @param language
	 * @param ht
	 * @param freq
	 */
	private static void addValues(String timestamp, String language, String ht, String freq) {
		byte[] key = generateKey(language, ht);
		Put put = new Put(key, Long.parseLong(timestamp));
		put.add(familyLang, columnName, Bytes.toBytes(language));
		put.add(familyHt, columnName, Bytes.toBytes(ht));
		put.add(familyHt, columnFreq, Bytes.toBytes(freq));
		try {
			hTableTwitter.put(put);
			if (DEBUG)
				System.out.println("Inserted: " + timestamp + " " + language + " , " + ht + " , " + freq);
		} catch (RetriesExhaustedWithDetailsException e) {

			e.printStackTrace();
		} catch (InterruptedIOException e) {

			e.printStackTrace();
		}

	}

	/**
	 * 
	 * Creates the keyrow of the table
	 * 
	 * @param language
	 * @param ht
	 * @return
	 */
	private static byte[] generateKey(String language, String ht) {
		byte[] key = new byte[KEYSIZE];
		System.arraycopy(Bytes.toBytes(language), 0, key, 0, language.length());
		System.arraycopy(Bytes.toBytes(ht), 0, key, 2, ht.length());
		return key;
	}

	/**
	 * Remove the table from HBase
	 * 
	 * @param admin
	 */
	private static void cleanTable(HBaseAdmin admin) {
		try {
			admin.disableTable(tableTwitter);
			admin.deleteTable(tableTwitter);
			if (DEBUG)
				System.out.println("Database Deleted.");
		} catch (IOException ex) {
			System.out.println("The table does not exist, no need to remove it");
		}
	}

	/**
	 * Writes in the file:
	 * 
	 * Output with results:
	 * 
	 * o One output file for each query to be stored in the folder specified
	 * with the outputFolder input parameter.
	 * 
	 * o Filenames must be: ID_query1.out, ID_query2.out, ID_query3.out
	 * 
	 * o File format: language, position, word, startTS, endTS where startTS and
	 * endTS are the ones used as input parameters
	 * 
	 * o In case of words with the same frequency the ranking is done according
	 * with the alphabetic order.
	 * 
	 * o Multiple executions of the same query must use the same file to store
	 * the results without overwriting previous results.
	 * 
	 * @param lang
	 * @param TopN
	 */
	private static void writeFile(String lang, String[][] TopN) {
		String filename = outputFolder + GROUP_ID + "_query" + mode + ".out";

		try {

			FileWriter fw = new FileWriter(filename, true);

			for (int i = 0; i < TopN.length; ++i) {
				String line = lang + "," + (i + 1) + "," + TopN[i][0] + "," + startTS + "," + endTS;

				// Append to the file
				fw.write(line);
				fw.write(System.getProperty("line.separator"));
				// fw.write(System.lineSeparator()); // Only > Java 7
			}

			if (DEBUG)
				System.out.println("File writted correctly.");

			fw.close();

		} catch (IOException e) {
			System.err.println("Error, Can not writte in the output file.");
			System.exit(-1);
		}
	}

	
	/**
	 * Prints Usage of the whole program
	 */
	private static void printUsageAndExit() {
		System.out.println("Usage: ");
		System.out.println("./hBaseApp.sh mode zkHost startTs endTs N Langauges dataFolder outputFolder");
		System.exit(-1);
	}

	/**
	 * Prints usage of the query 1
	 */
	private static void printQuery1Usage() {
		System.out.println("Usage of Mode 1: ");
		System.out.println("./hBaseApp.sh 1 zkHost startTS endTS N language outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}

	/**
	 * Prints usage of the query 2
	 */
	private static void printQuery2Usage() {
		System.out.println("Usage of Mode 2: ");
		System.out.println("./hBaseApp.sh 2 zkHost startTS endTS N language/s outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}

	/**
	 * Prints usage of the query 3
	 */
	private static void printQuery3Usage() {
		System.out.println("Usage of Mode 3: ");
		System.out.println("./hBaseApp.sh 3 zkHost startTS endTS N outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}

	/**
	 * Prints usage of the mode 4
	 */
	private static void printCreateDDBBUsage() {
		System.out.println("Usage of Mode 4: ");
		System.out.println("./hBaseApp.sh 4 zkHost dataFolder");
		System.exit(-1);
	}

}
