package master2016;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseApp {

	private static final boolean DEBUG = true;
	private static final boolean INFO = true;

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
	private static byte[] Table = Bytes.toBytes("twitterStats");
	private static byte[] Lang = Bytes.toBytes("lang");
	private static byte[] Ht = Bytes.toBytes("ht");

	// Columns
	private static byte[] Name = Bytes.toBytes("name");
	private static byte[] Freq = Bytes.toBytes("freq");

	/**
	 * 
	 * @param args
	 *            should be 8: 0-Mode: - 1: - 2: - 3: - 4: 1-zkHost: 2-startTS:
	 *            3-endTS: 4-N: 5-Languages: 6-dataFolder: 7-outputFolder:
	 */
	public static void main(String[] args) {

		// If there are no 8 parameters exit with error
		if (args.length != 8) {
			printUsageAndExit();
		}

		// Load parameters
		mode = Integer.valueOf(args[0]);
		zkHost = args[1];
		startTS = Long.valueOf(args[2]);
		endTS = Long.valueOf(args[3]);
		topN = Integer.valueOf(args[4]);
		languages = args[5].split(",");
		dataFolder = args[6];
		outputFolder = args[7];
		if (DEBUG) {
			System.out.println("Parameters: " + mode + ", " + zkHost + ", " + startTS + ", " + endTS + ", " + topN
					+ ", " + Arrays.toString(languages) + ", " + dataFolder + ", " + outputFolder);
		}

		boolean flag_ts_correct = endTS > startTS;

		// DO
		if (mode == 1 && flag_ts_correct) {
			runQuery1();
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

	/**
	 * Given a language (lang), find the Top-N most used words for the given
	 * language in a time interval defined with a start and end timestamp. Start
	 * and end timestamp are in milliseconds
	 * 
	 * @throws IOException
	 */
	public static void runQuery1() {
		if (languages.length != 1) {
			printQuery1Usage();
		}

		// Instantiating Configuration class
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class
		try {
			HTable table = new HTable(config, Table);

			// Instantiating the Scan class
			Scan scan = new Scan();

			Filter f = new SingleColumnValueFilter(Lang, Name, CompareFilter.CompareOp.EQUAL,Bytes.toBytes(languages[0]));
			
			// Scanning the required columns
			scan.addColumn(Lang, Name);
			scan.addColumn(Ht, Name);
			scan.addColumn(Ht, Freq);
			scan.setTimeRange(startTS, endTS);
			
			scan.setFilter(f);
			
			// Getting the scan result
			ResultScanner scanner = table.getScanner(scan);
			
			

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
		// TODO
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
			Configuration conf = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(conf);

			// First we clean the data
			// If is the first time, it will throw a TableNotFoundException
			admin.disableTable(Table);
			admin.deleteTable(Table);

			// And now we add the tables and families

			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(Table));
			HColumnDescriptor family_1 = new HColumnDescriptor(Lang);
			HColumnDescriptor family_2 = new HColumnDescriptor(Ht);
			family_1.setMaxVersions(10);
			family_2.setMaxVersions(10);
			table.addFamily(family_1);
			table.addFamily(family_2);
			admin.createTable(table);
		} catch (TableNotFoundException e) {
			// Do not do anything
		} catch (ZooKeeperConnectionException ex) {
			System.err.println("Error Connecting with the HBase.");
			Logger.getLogger(HbaseApp.class.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			System.err.println("Error Writting or reading in HBase.");
			Logger.getLogger(HbaseApp.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	private static void printUsageAndExit() {
		System.out.println("Usage: ");
		System.out.println("./hBaseApp.sh mode zkHost startTs endTs N Langauges dataFolder outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}

	private static void printQuery1Usage() {
		System.out.println("Usage of Mode 1: ");
		System.out.println("./hBaseApp.sh 1 zkHost startTS endTS N language outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}

}
