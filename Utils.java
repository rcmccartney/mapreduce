package mapreduce;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

/*
 * This class stores the constants used through out the project. 
 */
public class Utils {
	
	// Flag to turn on/off debug messages (mostly logs) on the server
	public static final boolean DEBUG = true;
	
	// The port numbers used for message passing
	public static final int BASE_WP2P_PORT = 40016,
							DEF_MASTER_PORT = 40001,
							DEF_CLIENT_PORT = 40000;
	
	//Command constants used in network communication
	public static final byte 
			NONE = 0,
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
			W2M_WP2P_PORT = 16,
			W2M_REDUCEDKV = 17,
			W2M_JOBDONE = 18,
			M2W_DATA_USAGE = 19,
			ACK = 20;
	
	//Path for flat directory, where each worker stores files
	public static final String basePath = "temp/";
	
	/**
	 * This method prints messages on to the console if DEBUG flag is on.
	 * @param  str String to print to standard error
	 */
	public static void debug(String str){
		if (DEBUG)
			System.err.println(str);
	}
	
	/**
	 * Turn two separate byte arrays into a single array 
	 * @param a the first byte array, will take the lower indexed position of the output array
	 * @param b the second byte array that takes the higher indexed positions
	 * @return byte[] c that is a combination of the two input arrays
	 */
	public static byte[] concat(byte[] a, byte[] b) {
		int aLen = a.length;
		int bLen = b.length;
		byte[] c = new byte[aLen+bLen];
		System.arraycopy(a, 0, c, 0, aLen);
		System.arraycopy(b, 0, c, aLen, bLen);
		return c;
	}
	
	/**
	 * Convert an integer into a 4-dimensional byte array
	 * @param value int to convert
	 * @return len 4 byte array  
	 */
	public static final byte[] intToByteArray(int value) { 
		return new byte[] { 
				(byte)(value >>> 24), 
				(byte)(value >>> 16), 
				(byte)(value >>> 8), 
				(byte) value}; 
	}
	
	/**
	 * Convert a byte array into an integer.  Assumes the array is of size 4
	 * @param b 4-byte array to be converted to an integer
	 * @return int value
	 */
	public static int byteArrayToInt(byte[] b) { 
		// here the and operator promotes the byte and the hex to int
		// and converts to the proper 8 bit value within the int without
		// basically overriding Java's signed byte value
		return  (b[0] << 24) + 
				((b[1] & 0xFF) << 16) + 
				((b[2] & 0xFF) << 8) + 
				(b[3] & 0xFF); 
	}
	
    /**
     * Function to get list of files over a socket
     * @param in the socket InputStream that will receive the filenames
     * @return list of filenames received.  empty list is returned if no filenames are sent
     */
 	public static List<String> readFilenames(InputStream in) {
		// length is the number of files being read
		int length = Utils.readInt(in);  
		List<String> list = new LinkedList<>();
		for(int i=0; i < length; i++) 
			list.add(readString(in));
		return list;
 	}
 	
 	/**
 	 * Helper function to read a String over a socket.  The String must be newline delimited
 	 * @param in stream to read from 
 	 * @return String read from the stream
 	 */
 	public static String readString(InputStream in)  {
 		try {
 			int f;
 			String name = "";
 			while((f = in.read()) != -1 && f != '\n') 
 				name += (char) f;
 			return name;
 		} catch (IOException e) {
			debug("Exception reading String from " + in.toString() + ": " + e);
			return "";
		}
 	}
 	
 	/**
 	 * Reads an integer from an input stream, where the int is transported as 
 	 * a 4-byte array 
 	 * 
 	 * @param in input stream to read from
 	 * @return int signed value, or -1 for failure
 	 */
	public static int readInt(InputStream in) {
		try {
			byte[] mybytearray = new byte[4];
			for(int i = 0; i < 4; i++)
				mybytearray[i] = (byte) in.read();
			return Utils.byteArrayToInt(mybytearray);
		} catch (IOException e) {
			debug("Exception reading int from " + in.toString() + ": " + e);
			return -1;
		}
	}
	
	/**
	 * Reads from an input stream and writes to an output stream simultaneously, using a 
	 * buffer of size 1024.
	 * 
	 * @param in Stream to read from
	 * @param out Stream to write to (usually to a File)
	 * @return int bytes read and written, or -1 for failure
	 */
	public static int readWriteBytes(InputStream in, OutputStream out) {
		try{		
			int totalBytes = 0;
			byte[] mybytearray = new byte[1024];
			while (true) {
				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
				totalBytes += bytesRead;
				if (bytesRead <= 0) break;
				out.write(mybytearray, 0, bytesRead);
				if (bytesRead < 1024) break; //assumes a single message is passed
			}
			out.flush();
			return totalBytes;
		} catch (IOException e) {
			debug("Exception in read/write from " + in.toString() + " to " + out.toString() + ": " + e);
			return -1;
		}
	}
	
	/**
	 * Receives a file over a Stream as a byte sequence, and writes it to the location 
	 * given as a parameter to the function
	 * 
	 * @param in stream to read from
	 * @param writeDir directory to write the file to
	 * @return String filename, or empty String for failure
	 */
	public static String receiveFile(InputStream in, String writeDir){
		try {
			// the first thing sent will be the filename
			String name = Utils.readString(in);
			BufferedOutputStream bos = new BufferedOutputStream(
					new FileOutputStream(writeDir + name));
			// this will write the bytes received to disk
			int totalCount = Utils.readWriteBytes(in, bos);
			bos.close();
			System.err.printf("%s: %d bytes downloaded%n", name, totalCount);
			return name;
		} catch (IOException e) {
			debug("Exception in receiving from " + in.toString() + ": " + e);
    		return "";
		}
	}
 	
 	/**
 	 * This function copies one file into another in a different destination on the local machine
 	 * 
 	 * @param sourceFile File to be copied
 	 * @param destFile File to become the copy of sourceFile
 	 * @throws IOException 
 	 */
	public static void copyFile(File sourceFile, File destFile) throws IOException {
	    if (!destFile.exists()) {
	        destFile.createNewFile();
	    }
	    FileChannel source = null;
	    FileChannel destination = null;
	    try {
	        source = new FileInputStream(sourceFile).getChannel();
	        destination = new FileOutputStream(destFile).getChannel();
	        destination.transferFrom(source, 0, source.size());
	    }
	    finally {
	    	if(source != null) {
	    		source.close();
	    	}
	        if(destination != null) {
	            destination.close();
	        }
	    }
	}
	
	/**
	 * Write the bytes of a file along with its filename to an output stream
	 * 
	 * @param out output stream to write the bytes of the file to
	 * @param command the type of file this is, a MR file or a regular file upload    
	 * @param filename name of file to store the bytes into
	 * @param barg the bytes of the file transferred over the stream
	 */
	public static void writeFile(OutputStream out, String filename, byte... barg) {
		try {
			//filename must be newline delimited from the bytes of data
			byte[] barr = Utils.concat((filename+'\n').getBytes(), barg);
			out.write(barr);
			out.flush();
		} catch (IOException e) {
			debug("Exception in writing file to " + out.toString() + ": " + e);
		}
	}
	
	/**
	 * Writes a list of filenames to an output stream, to be read by readFilenames
	 * @param out stream to write to 
	 * @param args String[] of filenames
	 */
	public static void writeFilenames(OutputStream out, String[] args) {
		try {
			// first write the number of filenames that will be sent
			out.write(Utils.intToByteArray(args.length));
			for (String s : args)
				out.write((s + "\n").getBytes());
			out.flush();
		} catch (IOException e) {
			debug("Exception in writing filenames to " + out.toString() + ": " + e);
		}
	}

    public static void writeObject(OutputStream out, Object obj) {
    	try {
    		ObjectOutputStream objStream = new ObjectOutputStream(out);
    		objStream.writeObject(obj);
    		objStream.flush();
    	} catch (IOException e) {
			debug("Exception in writing object to " + out.toString() + ": " + e);
    	}				
    }

    public static void writeCommand(OutputStream out, byte command, int jobID) {
    	try {
			out.write(command);
			out.write(Utils.intToByteArray(jobID));
    		out.flush();
    	} catch (IOException e) {
			debug("Exception in writing command to " + out.toString() + ": " + e);
    	}				
    }
    
    public static void writeInt(OutputStream out, int arg) {
    	try {
			out.write(Utils.intToByteArray(arg));
    		out.flush();
    	} catch (IOException e) {
			debug("Exception in writing int to " + out.toString() + ": " + e);
    	}			
    }
}