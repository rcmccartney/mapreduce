package mapreduce;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client implements Runnable {
	protected String hostName = ""; 
	protected int id;
	protected int port = Utils.DEF_MASTER_PORT;
	protected Socket socket;
	protected OutputStream out;
	protected InputStream in;
	//stopped is used by multiple threads, must be synchronized
	protected boolean stopped = false;
	
	public Client(String[] args) {
		if (args.length > 0)  
    		parseArgs(args);
    	try {
    		if (hostName.length() == 0) 
    			hostName = InetAddress.getLocalHost().getHostAddress();
    		socket = new Socket(hostName, port);
    		System.out.println("Worker " + socket);
            out = socket.getOutputStream();
            in = socket.getInputStream();
            id = in.read();  //first thing sent is worker ID
            // using port + 1 for Wp2p ensures it is unique for multiple instances
            new Thread(this).start();  //start a thread to read from the Master
		} catch (Exception e) {
			System.out.println("Cannot connect to the Master server at this time.");
			System.out.println("Did you specify the correct hostname and port of the server?");
		}
	}
	
	private void parseArgs(String args[]) {
	
	}
	
	public void sendJob(String jobFilePath, String...filePaths) {
		try {
			Path file = Paths.get(jobFilePath);
			byte[] filedata = Files.readAllBytes(file);
			writeMaster(Utils.C2M_UPLOAD);
    		writeMaster(file.getFileName().toString()+'\n', filedata);
			// otherwise data buffer reads into next message
    		try { Thread.sleep(10); } catch (Exception e) {}
    		writeMaster(Utils.C2M_UPLOAD_FILES);
    		writeMaster(filePaths.length);
    		for (String fileToUse : filePaths) {
    			writeMaster(fileToUse);
    		}
			System.out.println("Java file uploaded to Master server.");
    		closeConnection();
		} catch (IOException e) {
			System.out.println("Error loading MR file");
			closeConnection();
		}
	}
	
	public void writeMaster(String arg, byte... barg) {
    	try {
    		byte[] barr = Utils.concat(arg.getBytes(), barg);
    		out.write(barr);
    		out.flush();
    	} catch (IOException e) {
    		System.err.println("Error writing to Master: closing connection.");
    		this.closeConnection();
    	}
    }
    
    public void writeMaster(String arg) {
    	try {
    		out.write(arg.getBytes());
    		out.flush();
    	} catch (IOException e) {
    		System.err.println("Error writing to Master: closing connection.");
    		this.closeConnection();
    	}
    }
    
    public void writeMaster(byte... arg) {
    	try {
    		out.write(arg);
    		out.flush();
    	} catch (IOException e) {
    		System.err.println("Error writing to Master: closing connection.");
    		this.closeConnection();
    	}
    }

    public void writeMaster(int arg) {
    	try {
    		out.write(arg);
    		out.flush();
    	} catch (IOException e) {
    		System.err.println("Error writing to Master: closing connection.");
    		this.closeConnection();
    	}
    }
	
    public synchronized void closeConnection() {
    	stopped = true;
    	try {
    		socket.close();
    		in.close();
    		out.close();
    	} catch (IOException e) {} //ignore exceptions since you are quitting
    }
    
	 public synchronized boolean isStopped() {
	    	return stopped;
	    }
	    
	 public void receive(int command) {

			switch(command) {
			case Utils.MR_QUIT:  //quit command
	    		closeConnection();
	    		break;
			case Utils.M2C_RESULT:  //quit command
	    		receiveFile();
	    		closeConnection();
	    		break;
			case Utils.M2W_REQ_LIST:
				//Since it's client it wont reply with address
				break;
			default:
				System.err.println("Unrecognized Worker command: " + command);
				break;
			}
	    }
	 
	 //TO receive the final output file from master
	 //Master needs to send Utils.M2C_RESULT and then the results in a file
	 private String receiveFile(){
			try {
				// the first thing sent will be the filename
				int f;
				String name = "";
				while( (f = in.read()) != '\n') {
					name += (char) f;
				}
				// now read the actual byte array
				byte[] mybytearray = new byte[1024];
				BufferedOutputStream bos = new BufferedOutputStream(
						new FileOutputStream("Result\\" + File.separator + name));
				int totalCount = 0;
				while(true) {
					int bytesRead = in.read(mybytearray, 0, mybytearray.length);
					totalCount += bytesRead;
					if (bytesRead <= 0) break;
					bos.write(mybytearray, 0, bytesRead);
					if (bytesRead < 1024) break;
				}
				System.out.printf("%s %d bytes downloaded%n", name, totalCount);
				bos.close();
				return name;
			} catch (IOException e) {
	    		System.err.println("Error receiving file from Master: closing connection.");
	    		this.closeConnection();
	    		return null;
			}
		}
	 
	public void run() {
    	int command;
    	while(!isStopped()) {
    		try {
    			if ((command = in.read()) != 0)
    				this.receive(command);
			} catch (IOException e) {
				if (isStopped()) // exception is expected when the connection is first closed
					return;
				System.err.println("Error in socket connection to Master: closing connection");
				this.closeConnection();
			}
    	}
    }
	
	public static void main(String[] args) {
		//send a file from Desktop to the Master
		new Client(args).sendJob("M:\\mrockk\\eclipse\\mapreduce\\bin\\MRTest2.java", "M:\\BDA\\data\\data_5440.txt"); 
	}
}