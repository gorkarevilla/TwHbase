package master2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.hbase.client.Put;
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
	private static byte[] CF_1 = Bytes.toBytes("lang");
	private static byte[] CF_2 = Bytes.toBytes("ht");

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
					+ ", " + languages + ", " + dataFolder + ", " + outputFolder);
		}

		boolean flag_ts_correct = endTS < startTS;

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
	 */
	public static void runQuery1() {
		// TODO
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
			HColumnDescriptor family_1 = new HColumnDescriptor(CF_1);
			HColumnDescriptor family_2 = new HColumnDescriptor(CF_2);
			family_1.setMaxVersions(100);
			family_2.setMaxVersions(100);
			table.addFamily(family_1);
			table.addFamily(family_2);
			admin.createTable(table);
                        
                        //Finally we read from the file and add the values
                        

                        File folder = new File(dataFolder);
                        File[] listOfFiles = folder.listFiles();

                        for (File file : listOfFiles) {
                            if (file.isFile() && file.getName().substring(file.getName().lastIndexOf('.')).equals(".out")) {
                                FileInputStream fstream = new FileInputStream(file.getName());
                                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

                                String strLine;

                                //Read File Line By Line
                                while ((strLine = br.readLine()) != null)   {
                                  String[] values = strLine.split(",");
                                  addValues(values[0], values[1], values[2], values[3]);
                                  addValues(values[0], values[1], values[4], values[5]);
                                  addValues(values[0], values[1], values[6], values[7]);
                                }

                                //Close the input stream
                                br.close();
                                }
                        }

		} catch (TableNotFoundException e) {
			System.out.println("ERR: The table not does not exist");
                        printUsageAndExit();
		} catch (ZooKeeperConnectionException ex) {
			System.out.println("Error Connecting with the HBase.");
                        printUsageAndExit();
		} catch (IOException ex) {
			System.out.println("Error Writting or reading in HBase.");
                        printUsageAndExit();
		}
	}

	private static void printUsageAndExit() {
		System.out.println("Usage: ");
		System.out.println("./hBaseApp.sh mode zkHost startTs endTs N Langauges dataFolder outputFolder");
		System.out.println("Be aware that endTs must be greater than startTs");
		System.exit(-1);
	}

    private static void addValues(String timestamp, String language, String ht, String freq) {
        byte[] key = generateKey(language, ht);
        Put put = new Put(key, Long.parseLong(timestamp));
        put.add(CF_1, Bytes.toBytes("name"), Bytes.toBytes(language));
        put.add(CF_2, Bytes.toBytes("name"), Bytes.toBytes(ht));
        put.add(CF_2, Bytes.toBytes("freq"), Bytes.toBytes(freq));
        
    }

    private static byte[] generateKey(String language, String ht) {
        byte[] key = new byte[42];
        System.arraycopy(Bytes.toBytes(language),0,key,0,language.length());
        System.arraycopy(Bytes.toBytes(ht),0,key,2,ht.length());
        return key;
    }

}
