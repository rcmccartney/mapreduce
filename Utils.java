package mapreduce;

/*
 * This class stores the constants used through out the project. 
 */
public class Utils {
	
	// Flag to turn on/off debug messages (mostly logs) on the server
	public static final boolean DEBUG = true;
	
	public static final int BASE_WP2P_PORT = 40016,
							DEF_MASTER_PORT = 40001;
	
	//Command constants used in network communication
	public static final byte 
			C2M_UPLOAD = 1, 
			C2M_UPLOAD_FILES = 2,
			M2W_MR_UPLOAD = 3,
			MR_QUIT = 4,
			W2M_KEY = 5, 
			W2M_KEY_COMPLETE = 6, 
			W2W_KEY_TRANSFER = 7,
			W2M_RESULTS = 8,
			M2W_REQ_LIST = 9,
			M2W_REQ_LIST_OKAY = 10,
			M2W_FILE = 11,
			M2W_COORD_KEYS = 12,
			W2W_KEY_TRANSFER_OKAY = 13,
			W2M_KEYSHUFFLED = 14,
			M2W_BEGIN_REDUCE = 15,
			W2M_WP2P_PORT = 16;

	
	//Path for flat directory, where each worker stores files
	public static final String basePath = "temp/";
	
	/*
	 * This method prints messages on to the console if DEBUG flag is on.
	 */
	public static void debug(String str){
		if(DEBUG)
			System.out.println(str);
	}
	
	public static byte[] concat(byte[] a, byte[] b) {
		int aLen = a.length;
		int bLen = b.length;
		byte[] c= new byte[aLen+bLen];
		System.arraycopy(a, 0, c, 0, aLen);
		System.arraycopy(b, 0, c, aLen, bLen);
		return c;
	}
	
	public static final byte[] intToByteArray(int value) { 
		return new byte[] { 
				(byte)(value >>> 24), 
				(byte)(value >>> 16), 
				(byte)(value >>> 8), 
				(byte)value}; 
	}
	
	public static int byteArrayToInt(byte [] b) { 
		return  (b[0] << 24) + 
				((b[1] & 0xFF) << 16) + 
				((b[2] & 0xFF) << 8) + 
				(b[3] & 0xFF); 
	}
}