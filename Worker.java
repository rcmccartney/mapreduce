package mapreduce;
//******************************************************************************
//File:    Worker.java
//Package: None
//Unit:    Distributed Programming Group Project
//******************************************************************************

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

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
	protected int port = 40001;
	protected Socket socket;
	protected OutputStream out;
	protected BufferedReader in;
	protected boolean stopped = false;
    
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
            out = socket.getOutputStream();
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
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
    	} catch (IOException e) {
    		System.err.println("Error writing to Master: closing connection.");
    		this.closeConnection();
    	}
    }
    
    public synchronized boolean isStopped() {
    	return stopped;
    }
    
    public void receive(String command) {
    	if (command.equals("q"))
    		closeConnection();
    }

    /**
     * 
     */
    public synchronized void closeConnection() {
    	stopped = true;
    	try {
    		socket.close();
    		in.close();
    		out.close();
    	} catch (IOException e) {} //ignore exceptions since you are quitting
    }
    
    public void run() {
    	String command;
    	while(!isStopped()) {
    		try {
    			if ( (command = in.readLine()) != null)
    				this.receive(command);
			} catch (IOException e) {
				if (isStopped()) // exception is expected when the connection is first closed
					return;
				System.err.println("Error in socket connection to Master: closing connection");
				this.closeConnection();
			}
    	}
    }
    
    @Override
    public String toString() {
    	return socket.toString();
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
				System.out.println("Correct usage: java Worker [-host <hostName>] [-p <portnumber>]");
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

