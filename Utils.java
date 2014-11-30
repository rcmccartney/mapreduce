package mapreduce;

import com.google.gson.Gson;

/*
 * This class stores the constants used through out the project. 
 */
public class Utils {
	
	// Flag to turn on/off debug messages (mostly logs) on the server
	public static final boolean DEBUG = true;
	
	// When reading a file over byte stream from a client it is stored to this location
	public static final String TMP_FILE = "MR_tmp.java";
	
	public static final int DEF_WP2P_PORT = 40016,
							DEF_MASTER_PORT = 40001;
	
	//Command constants used in network communication
	public static final byte 
			C2M_UPLOAD = 'C', 
			C2M_UPLOAD_OKAY = 'C',
			M2W_UPLOAD = 'W',
			M2W_UPLOAD_OKAY = 'W',
			MR_QUIT = 'Q',
			W2M_KEY = 'L', 
			W2M_KEY_OKAY = 'L',
			M2W_KEYASSIGN = 'M',
			M2W_KEYASSIGN_OKAY = 'M',
			W2W_KEY_TRANSFER = 'K',
			W2W_KEY_TRANSFER_OKAY = 'K',
			W2M_RESULTS = 'R',
			W2M_RESULTS_OKAY = 'R';
	
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