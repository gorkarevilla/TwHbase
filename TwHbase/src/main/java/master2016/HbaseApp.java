package master2016;

public class HbaseApp {

	private static final boolean DEBUG = true;
	private static final boolean INFO = true;

	// Parameters
	private static int mode;
	private static String zkHost;
	private static long startTS;
	private static long endTS;
	private static int topN;
	private static String languages;
	private static String dataFolder;
	private static String outputFolder;

	/**
	 * 
	 * @param args
	 *            should be 8: 0-Mode: - 1: - 2: - 3: - 4: 1-zkHost: 2-startTS:
	 *            3-endTS: 4-N: 5-Languages: 6-dataFolder: 7-outputFolder:
	 */
	public static void main(String[] args) {

		// If there are no 8 parameters exit with error
		if (args.length != 8) {
			if (INFO)
				System.out.println(
						"Needed 8 parameters: Mode, zkHost, startTS, endTS, N, languages, dataFolder, outputFolder");
			System.exit(-1);
		}

		// Load parameters
		mode = Integer.valueOf(args[0]);
		zkHost = args[1];
		startTS = Long.valueOf(args[2]);
		endTS = Long.valueOf(args[3]);
		topN = Integer.valueOf(args[4]);
		languages = args[5];
		dataFolder = args[6];
		outputFolder = args[7];
		if (DEBUG) {
			System.out.println("Parameters: " + mode + ", " + zkHost + ", " + startTS + ", " + endTS + ", " + topN
					+ ", " + languages + ", " + dataFolder + ", " + outputFolder);
		}

		// DO
		if (mode == 1) {
			runQuery1();
		} else if (mode == 2) {
			runQuery2();
		} else if (mode == 3) {
			runQuery2();
		} else if (mode == 4) {
			createDDBB();
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
		// TODO
	}

}
