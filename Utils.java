package mapreduce;

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
			C2M_UPLOAD = 'C', 
			C2M_UPLOAD_FILES = 'B',
			M2W_MR_UPLOAD = 'W',
			MR_QUIT = 'Q',
			W2M_KEY = 'L', 
			W2M_KEY_COMPLETE = 'Z', 
			M2W_KEYASSIGN = 'M',
			W2W_KEY_TRANSFER = 'K',
			W2M_RESULTS = 'R',
			M2W_REQ_LIST = 'F',
			M2W_REQ_LIST_OKAY = 'F',
			M2W_FILE = 'A',
			M2W_COORD_KEYS = 10,
			W2W_KEY_TRANSFER_OKAY = 11,
			W2M_KEYSHUFFLED = 12,
			M2W_BEGIN_REDUCE = 12;

	
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