package mapreduce;
//******************************************************************************
//File:    Worker.java
//Package: None
//Unit:    Distributed Programming Group Project
//******************************************************************************

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import com.sun.org.apache.xalan.internal.xsltc.compiler.CompilerException;

/**
 * This class parses command-line input in order to register client as a worker in a 
 * MapReduce architecture.  Once connected, the nodes send heartbeats to each other 
 * as a connected distributed system.  This node will then wait to be sent jobs 
 * by the Master node.  
 * 
 * 
 * @author rob mccartney
 */
public class Worker implements Runnable {

	protected String hostName = ""; 
	protected int port = Utils.DEF_MASTER_PORT;
	protected Socket socket;
	protected OutputStream out;
	protected InputStream in;
	//stopped is used by multiple threads, must be synchronized
	protected boolean stopped = false;
	protected Job<?, ?> currentJob;
	// TODO job queue 
    
    /**
     * Constructor that makes a new worker and attempts to register with a Master.
     * Master node must be already running
     * 
     * @param args String[] from the command line
     */
    public Worker(String[] args) {
    	if (args.length > 0)  
    		parseArgs(args);
    	try {
    		if (hostName.length() == 0) 
    			hostName = InetAddress.getLocalHost().getHostAddress();
    		socket = new Socket(hostName, port);
    		System.out.println("Worker " + socket);
            out = socket.getOutputStream();
            in = socket.getInputStream();
            new Thread(this).start();  //start a thread to read from the Master
		} catch (Exception e) {
			System.out.println("Cannot connect to the Master server at this time.");
			System.out.println("Did you specify the correct hostname and port of the server?");
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

    public synchronized boolean isStopped() {
    	return stopped;
    }
    
    public String toString() {
    	return socket.toString();
    }

    public synchronized void closeConnection() {
    	stopped = true;
    	try {
    		if (currentJob != null) 
    			currentJob.stopExecution();
    		socket.close();
    		in.close();
    		out.close();
    	} catch (IOException e) {} //ignore exceptions since you are quitting
    }
    
	// TODO add it so User can send already compiled classes over byte stream
	private Mapper<?, ?> compileAndLoadMRFile(String filename){
		
		try {	
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();  
			if (compiler == null)  // needs to be a JDK java.exe to have a compiler attached
				throw new CompilerException("Error: no compiler set for MR file");
			// zero means compile success
			int compilationResult = compiler.run(null, null, null, filename); 
			System.out.println(filename + " compilation " + (compilationResult==0?"successful":"failed"));
			// Class name is the filename before '.java'
			String className = filename.split("\\.")[0];
			Class<?> myClass = ClassLoader.getSystemClassLoader().loadClass(className); 
			Mapper<?, ?> mr = (Mapper<?, ?>) myClass.newInstance();
			// clean up the files you created
			Files.delete(Paths.get(filename));
			Files.delete(Paths.get(className + ".class"));
			return mr;
		} catch (Exception e) {
			System.err.println("Exception loading or compiling the File: " + e);
			return null;
		}
	} 			
    
	private String receiveMRFile(){
		System.out.print("Worker received new MR job: ");
		try{
			// the first thing sent will be the filename
			int f;
			String name = "";
			while( (f = in.read()) != '\n') {
				name += (char) f;
			}
			// now read the actual byte array
			byte[] mybytearray = new byte[1024];
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(name));
			int totalCount = 0;
			while(true) {
				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
				totalCount += bytesRead;
				if (bytesRead <= 0) break;
				bos.write(mybytearray, 0, bytesRead);
				if (bytesRead < 1024) break;
			}
			System.out.printf("%d bytes downloaded%n", totalCount);
			bos.close();
			return name;
		} catch (IOException e) {
    		System.err.println("Error receiving MR file from Master: closing connection.");
    		this.closeConnection();
    		return null;
		}
	}
    
    public void receive(int command) {

		switch(command) {
		case Utils.MR_QUIT:  //quit command
    		closeConnection();
    		break;
		case Utils.M2W_KEYASSIGN:	
			currentJob.receiveKeyAssignments();
			break;
		case Utils.M2W_UPLOAD:
			// TODO filesystem that can take in actual file names instead of "here", "there"
			Mapper<?, ?> mr = compileAndLoadMRFile(receiveMRFile());
			if (mr != null) {
				currentJob = new Job<>(this, mr, "here", "there");
				currentJob.begin();
			}
			break;		
		default:
			System.err.println("Unrecognized Worker command: " + command);
			break;
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
    
    /**
	 * This method parses any inputs for the port to use, and stores it into
	 * the instance variable prior to the constructor
	 * 
	 * @param args passed in on command line
	 */
	private void parseArgs(String args[]) {
		
		for (int i = 0; i < args.length; i ++) {	
			if (args[i].equals("-port")) 
				port = new Integer(args[++i]).intValue();
			else if (args[i].equals("-host")) 
				hostName = args[++i];
			else {
				System.out.println("Correct usage: java Worker [-host <hostName>] [-p <portnumber>] [");
				System.out.println("\t-host: override localhost to set the host to <hostName>.");
				System.out.println("\t-port: override default port 40001 to <port>.  "
						+ "\n\t<host> and <port> must match the Master Server's.");
				System.exit(1);
			}
		}
	}
	
	/**
	 * @param args for hostname or port to not be default
	 * @throws RemoteException 
	 */
	public static void main(String[] args) {
		new Worker(args);
	}
}

