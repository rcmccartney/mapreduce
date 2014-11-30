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
			C2M_UPLOAD = 'C', 
			M2W_UPLOAD = 'W',
			MR_QUIT = 'Q',
			W2M_KEY = 'L', 
			W2M_KEY_COMPLETE = 'Z', 
			M2W_KEYASSIGN = 'M',
			W2W_KEY_TRANSFER = 'K',
			W2M_RESULTS = 'R';
	
	// Reference to a reusable GSON object that's used in parsing to / from GSON
	public static final Gson gson = new Gson(); 
	
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