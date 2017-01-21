package master2016;

public class HbaseApp {

	private static final boolean DEBUG = true;
	private static final boolean INFO = true;

	
	//Parameters
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
 
		//If there are no 8 parameters exit with error
		if (args.length != 8) {
			if (INFO)
				System.out.println(
						"Needed 8 parameters: Mode, zkHost, startTS, endTS, N, languages, dataFolder, outputFolder");
			System.exit(-1);
		}

		//Load parameters
		mode = Integer.valueOf(args[0]);
		zkHost = args[1];
		startTS = Long.valueOf(args[2]);
		endTS = Long.valueOf(args[3]);
		topN = Integer.valueOf(args[4]);
		languages = args[5];
		dataFolder = args[6];
		outputFolder = args[7];
		if(DEBUG){
			System.out.println("Parameters: "
					+mode+","
					+zkHost+","
					+startTS+","
					+endTS+","
					+topN+","
					+languages+","
					+dataFolder+","
					+outputFolder+",");
		}
		
	}

}
