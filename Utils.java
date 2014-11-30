package mapreduce;

import com.google.gson.Gson;

/*
 * This class stores the constants used through out the project. 
 */
public class Utils {
	
	// Flag to turn on/off debug messages (mostly logs) on the server
	public static final boolean DEBUG = true;
	
	public static final int DEF_WP2P_PORT = 40016,
			DEF_MASTER_PORT = 40001;
	
	//Command constants used in network communication
	public static final byte 
			MR_C = 'C', 
			MR_C_OKAY = 'D',
			MR_W = 'W',
			MR_W_OKAY = 'X',
			MR_QUIT = 'Q',
			MR_MASTER_KEY = 'M',
			W2W_K = 'K',
			W2W_K_OKAY = 'L',
			REQ_LIST = 'R';
	
	//Path for flat directory, where each worker stores files
	public static String filePath = "temp/";
	
	// Reference to a reusable GSON object that's used in parsing to / from GSON
	public static final Gson gson = new Gson(); 
	
	/*
	 * This method prints messages on to the console if DEBUG flag is on.
	 */
	public static void debug(String str){
		if(DEBUG)
			System.out.println(str);
	}
}