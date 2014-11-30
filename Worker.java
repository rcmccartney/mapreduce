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

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

//import com.sun.org.apache.xalan.internal.xsltc.compiler.CompilerException;

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
	protected boolean stopped = false;
	protected Job<?, ?> currentJob;
	// TODO job queue & increment UDP port for different jobs
    
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
            //port to talk to other workers is first message sent
            new Thread(this).start();  //start a thread to read from the Master
		} catch (Exception e) {
			System.out.println("Cannot connect to the Master server at this time.");
			System.out.println("Did you specify the correct hostname and port of the server?");
			System.out.println("Please try again later.");
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
    
	// TODO change this to already compiled classes sent over byte stream
	private Mapper<?, ?> compileAndLoadMRFile(){
		try {	
			//be sure to change "java.exe" to point to the one in JDK not in JRE, else the compiler ref comes back as null  
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();  
			if (compiler == null)
				//throw new CompilerException("Error: no compiler set for MR file");
				throw new Exception(); 
			int compilationResult = compiler.run(null, null, null, "MR.java");  
			System.out.println("Compilation " + (compilationResult==0? "successful" : "failed") ); // zero means compile success
			// TODO: let the client sent the class name and load that here
			Class<?> myClass = ClassLoader.getSystemClassLoader().loadClass("MR"); 
			return (Mapper<?, ?>) myClass.newInstance();
		} catch (Exception e) {
			System.err.println("Exception loading or compiling the File: " + e);
			return null;
		}
	} 			
    
	private void receiveMRFile(){
		System.out.print("Worker received new MR job: ");
		try{
			byte[] mybytearray = new byte[1024];
			// TODO NOT cool that MR.java is hard-coded
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("MR.java"));
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
		} catch (IOException e) {
    		System.err.println("Error receiving MR file from Master: closing connection.");
    		this.closeConnection();
		}
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
    
    public void receive(int command) {

		switch(command) {
		case mapreduce.Utils.MR_QUIT:  //quit command
    		closeConnection();
    		break;
		case mapreduce.Utils.MR_MASTER_KEY:	
			currentJob.receiveKeyAssignment();
			break;
		case mapreduce.Utils.MR_W:
			writeMaster(mapreduce.Utils.MR_W_OKAY);
			receiveMRFile(); 
			// TODO need to create a new Job here with this Mapper that can use the Generics properly!!
			new Job<>(this, compileAndLoadMRFile(), "here", "there");
			break;
		default:
			System.err.println("Unrecognized command: " + command);
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

