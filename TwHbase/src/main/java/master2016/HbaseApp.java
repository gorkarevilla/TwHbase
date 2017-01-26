package master2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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

public class HbaseApp {

	// Prints to be displayed
	private static final boolean DEBUG = true;

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
			runQuery1(languages);
		} else if (mode == 2 && flag_ts_correct) {
			runQuery2();
		} else if (mode == 3 && flag_ts_correct) {
			runQuery2();
		} else if (mode == 4) {
			createDDBB();
		} else {
			printUsageAndExit();
		}

	}

	private static void connectHBase() {
		String[] host = zkHost.split(":");

		try {
			hConfiguration = HBaseConfiguration.create();
			hConfiguration.set("hbase.zookeeper.quorum", host[0]);
			hConfiguration.setInt("hbase.zookeeper.property.clientPort",Integer.valueOf(host[1]));
			
			hAdmin = new HBaseAdmin(hConfiguration);
			hTableTwitter = new HTable(hConfiguration, TABLENAME);
		}catch (NumberFormatException e) {
			System.err.println("Error, zkHost parameter is not properly written, check it.");
			System.exit(-1);
		}catch (MasterNotRunningException e) {
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
	public static void runQuery1(String[] languages) {
		if (languages.length != 1) {
			printQuery1Usage();
		}

		try {

			// Instantiating the Scan class
			Scan scan = new Scan();

			Filter f = new SingleColumnValueFilter(familyLang, columnName, CompareFilter.CompareOp.EQUAL,
					Bytes.toBytes(languages[0]));

			// Scanning the required columns
			scan.addColumn(familyLang, columnName);
			scan.addColumn(familyHt, columnName);
			scan.addColumn(familyHt, columnFreq);
			scan.setTimeRange(startTS, endTS);
			scan.setMaxVersions(MAXVERSIONS);

			scan.setFilter(f);

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

			String[][] TopN = getTopNArray(topN);

			if (DEBUG)
				System.out.println(Arrays.deepToString(TopN));

		} catch (TableNotFoundException e) {
			System.err.println("Error, Table is not created, try with mode 4.");

		} catch (IOException e) {
			System.err.println("Error, Can not read from the database");
			// e.printStackTrace();
		}
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
			String[] l = new String[1];
			l[0] = lang;
			runQuery1(l);

		}
	}

	/**
	 * Find the Top-N most used words and the frequency of each word regardless
	 * the language in a time interval defined with the provided start and end
	 * timestamp. Start and end timestamp are in milliseconds.
	 */
	public static void runQuery3() {
		// TODO
	}

	/**
	 * Create and load data from the files. File names have the format
	 * “lang.out”, for example en.out, it.out, es.out
	 */
	public static void createDDBB() {

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

		if(DEBUG)
			System.out.println("Adding: "+hashtag+","+freq);
			
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

		if (DEBUG) {
			for (int c = 0; c < n; ++c) {
				System.out.println("=> Top" + c + ": " + topN[c][0] + ":" + topN[c][1]);
			}
		}

		return topN;
	}

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

	private static byte[] generateKey(String language, String ht) {
		byte[] key = new byte[KEYSIZE];
		System.arraycopy(Bytes.toBytes(language), 0, key, 0, language.length());
		System.arraycopy(Bytes.toBytes(ht), 0, key, 2, ht.length());
		return key;
	}

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

	private static void printUsageAndExit() {
		System.out.println("Usage: ");
		System.out.println("./hBaseApp.sh mode zkHost startTs endTs N Langauges dataFolder outputFolder");
		System.exit(-1);
	}

	private static void printQuery1Usage() {
		System.out.println("Usage of Mode 1: ");
		System.out.println("./hBaseApp.sh 1 zkHost startTS endTS N language outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}

	private static void printQuery2Usage() {
		System.out.println("Usage of Mode 2: ");
		System.out.println("./hBaseApp.sh 2 zkHost startTS endTS N language/s outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}
	
	private static void printQuery3Usage() {
		System.out.println("Usage of Mode 3: ");
		System.out.println("./hBaseApp.sh 3 zkHost startTS endTS N outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}
	
	private static void printCreateDDBBUsage() {
		System.out.println("Usage of Mode 4: ");
		System.out.println("./hBaseApp.sh 4 zkHost dataFolder");
		System.exit(-1);
	}
	

}
