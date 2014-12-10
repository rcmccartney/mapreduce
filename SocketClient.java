package mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

public class SocketClient {

	protected String hostName = ""; 
	protected int id;
	protected int port = Utils.DEF_MASTER_PORT;
	protected Socket socket;
	protected OutputStream out;
	protected InputStream in;
    
    /**
     * Constructor that makes a new SocketClient and attempts to register with 
     * the given hosname and port
     * 
     * @param args String[] from the command line
     */
    public SocketClient(String[] args) {
    	if (args.length > 0)  
    		parseArgs(args);
    	try {
    		if (hostName.length() == 0) 
    			hostName = InetAddress.getLocalHost().getHostAddress();
    		socket = new Socket(hostName, port);
            out = socket.getOutputStream();
            in = socket.getInputStream();
            id = in.read();  //first thing sent is a client ID
    		System.out.println(this + ": " + id);
    	}
		catch (Exception e) {
			System.out.println("Cannot connect to the server at this time");
			System.out.println("Verify that you have the correct hostname and port");
			System.exit(1);
		}
    }

    public synchronized void closeConnection() {
    	try {
    		in.close();
    		out.close();
    		socket.close();
    	} catch (IOException e) {} //ignore exceptions since you are quitting
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
}