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
	
	public static final int BASE_WP2P_PORT = 40016,
							DEF_MASTER_PORT = 40001,
							DEF_CLIENT_PORT = 40000;
	
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
			W2M_WP2P_PORT = 16,
			W2M_REDUCEDKV = 17,
			W2M_JOBDONE = 18,
			M2W_DATA_USAGE = 19,
			AWK = 20;

	
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
				(byte) value}; 
	}
	
	public static int byteArrayToInt(byte [] b) { 
		return  (b[0] << 24) + 
				((b[1] & 0xFF) << 16) + 
				((b[2] & 0xFF) << 8) + 
				(b[3] & 0xFF); 
	}
	
    /**
     * Function to get list of files over a socket
 	 * 
     * @param in the socket InputStream that will receive the filenames
     * @return list of file names received
     */
 	public static List<String> getFilesList(InputStream in) {
 		try {
 			// length is the number of files being read
 			int length = in.read();  // TODO won't work for more than 1 byte of files
 			List<String> list = new LinkedList<>();
 			for(int i=0; i < length; i++) 
 				list.add(readString(in));
 			return list;
 		} catch (IOException e) {
 			System.err.println("Exception while receiving file listing from Client: " + e);
 			return null;
 		}
 	}
 	
 	public static String readString(InputStream in) throws IOException {
 		try {
 			int f;
 			String name = "";
 			while( (f = in.read()) != '\n') 
 				name += (char) f;
 			return name;
 		} catch (IOException e) {
			System.err.println("Error reading String: " + e);
			return "";
		}
 	}
 	
	public static int readInt(InputStream in) {
		try {
			byte[] mybytearray = new byte[4];
			for(int i = 0; i < 4; i++)
				mybytearray[i] = (byte) in.read();
			return Utils.byteArrayToInt(mybytearray);
		} catch (IOException e) {
			System.err.println("Error reading int: " + e);
			return -1;
		}
	}
	
	public static int readWriteBytes(InputStream in, OutputStream out) {
		try{		
			int totalBytes = 0;
			byte[] mybytearray = new byte[1024];
			//ByteArrayOutputStream bos = new ByteArrayOutputStream();
			while (true) {
				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
				totalBytes += bytesRead;
				if (bytesRead <= 0) break;
				out.write(mybytearray, 0, bytesRead);
				if (bytesRead < 1024) break; //assumes a single message is passed
			}
			out.flush();
			//return bos.toByteArray();
			return totalBytes;
		} catch (IOException e) {
			System.err.println("Exception while receiving file from Client: " + e);
			return -1;
		}
	}
	
	public static String receiveFile(InputStream in, String writeDir){
		try {
			// the first thing sent will be the filename
			System.err.print("...Receiving file: ");
			String name = Utils.readString(in);
			BufferedOutputStream bos = new BufferedOutputStream(
					new FileOutputStream(writeDir + name));
			// this will write the bytes received to disk
			int totalCount = Utils.readWriteBytes(in, bos);
			bos.close();
			System.err.printf("%s %d bytes downloaded%n", name, totalCount);
			return name;
		} catch (IOException e) {
    		System.err.println("Error receiving file from Master: closing connection.");
    		return null;
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
	    if(!destFile.exists()) {
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
	
	public static void write(OutputStream out, byte command, String arg, byte... barg) {
		try {
			byte[] barr = Utils.concat(arg.getBytes(), barg);
			out.write(command);
			out.write(barr);
			out.flush();
		} catch (IOException e) {
			System.err.printf("Error writing to OutputStream: " + e);
		}
	}
	
	public static void write(OutputStream out, byte command, String[] args) {
		try {
			out.write(command);
			out.write(args.length);  //won't work for > 1 byte of Strings
			for (String s : args)
				out.write((s + "\n").getBytes());
			out.flush();
		} catch (IOException e) {
			System.err.printf("Error writing to OutputStream: " + e);
		}
	}

    public static void write(OutputStream out, byte command, Object obj) {
    	try {
			out.write(command);
    		ObjectOutputStream objStream = new ObjectOutputStream(out);
    		objStream.writeObject(obj);
    		objStream.flush();
    	} catch (IOException e) {
			System.err.printf("Error writing to OutputStream: " + e);
    	}				
    }

    public static void write(OutputStream out, byte command, String arg) {
    	try {
			out.write(command);
			out.write((arg + "\n").getBytes());
    		out.flush();
    	} catch (IOException e) {
			System.err.printf("Error writing to OutputStream: " + e);
    	}				
    }

    public static void write(OutputStream out, byte command, byte... arg) {
    	try {
			out.write(command);
			out.write(arg);
    		out.flush();
    	} catch (IOException e) {
			System.err.printf("Error writing to OutputStream: " + e);
    	}				
    }
    
    public static void writeCommand(OutputStream out, byte command) {
    	try {
			out.write(command);
    		out.flush();
    	} catch (IOException e) {
			System.err.printf("Error writing to OutputStream: " + e);
    	}				
    }
    
    // this was messing up writing a single byte
    public static void write(OutputStream out, byte command, int arg) {
    	try {
			out.write(command);
			out.write(arg);
    		out.flush();
    	} catch (IOException e) {
			System.err.printf("Error writing to OutputStream: " + e);
    	}
    }
    
    public static void writeInt(OutputStream out, byte command, int arg) {
    	Utils.write(out, command, intToByteArray(arg));
    }
}