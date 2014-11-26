package mapreduce;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;


public class Master extends Thread {

	private ServerSocket aServerSocket;
	private int port = 40001;
	private BufferedReader in;
	private PrintWriter out;

	public Master(String[] args)	{
		if (args.length > 0)
			parseArgs(args);
		try { 
			aServerSocket = new ServerSocket(port);
			System.out.println ("Listening on port: " + aServerSocket.getLocalPort());
		} catch(Exception e) {
			System.out.println(e);
		}
	}
	
	/**
     * 
     * @throws Exception
     */
	public void closeIOconections() throws Exception {
		try {
			in.close();
    		out.close();
    	} catch (Exception e )       {
    		System.out.println(e.toString());
    		System.exit(1);
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
			else {
				System.out.println("Correct usage: java Master [-p <portnumber>]");
				System.out.println("\t-port: override default port 40001 to <port>.");
				System.exit(1);
			}
		}
	}
	
	public void run()	{
		try {
			Socket ssock = aServerSocket.accept();
			System.out.println(ssock.toString());
			out = new PrintWriter(ssock.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(ssock.getInputStream()));
			
		} catch(Exception e) {
            System.out.println(e);
            e.printStackTrace();
		}
	}
   
   public static void main(String[] args) {
	   new Master(args).start();
   }
}
