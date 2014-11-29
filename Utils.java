package mapreduce;

/*
 * This class stores the constants used through out the project. 
 */
public class Utils {
	
	// Flag to turn on/off debug messages (mostly logs) on the server
	public static final boolean DEBUG = true; 
	
	//Command constants used in network communication
	public static final byte 
			MR_C = 'C', 
			MR_C_OKAY = 'D',
			MR_W = 'W',
			MR_W_OKAY = 'X',
			MR_QUIT = 'Q',
			MR_MASTER_KEY = 'M';
	
	/*
	 * This method prints messages on to the console if DEBUG flag is on.
	 */
	public static void debug(String str){
		if(DEBUG)
			System.out.println(str);
	}
}