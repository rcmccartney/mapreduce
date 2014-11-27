package mapreduce;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class Master extends Thread {

	protected ServerSocket serverSocket;
	protected int port = 40001;
	protected boolean stopped = false;
	protected static int id_counter = 0;
	protected Collection<WorkerConnection> workerQueue; 
	
	public Master(String[] args)	{
		workerQueue = new ArrayList<>();
		if (args.length > 0)
			parseArgs(args);
		try { 
			serverSocket = new ServerSocket(port);
			System.out.println ("Listening on port: " + serverSocket.getLocalPort());
		} catch(Exception e) {
            throw new RuntimeException("Cannot open port " + port, e);
		}
	}
	
	public void receive(String command, int id) {
		// TODO Auto-generated method stub
		
	}
	
    private synchronized boolean isStopped() {
        return stopped;
    }

    public synchronized void stopServer() {
        this.stopped = true;
        try {
            this.serverSocket.close();
            for (WorkerConnection conn : workerQueue)
            	conn.closeConnection();
        } catch (IOException e) {
            throw new RuntimeException("Error closing master", e);
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
	
	public void remove(int workerID) {
		Iterator<WorkerConnection> it = workerQueue.iterator();
		while (it.hasNext()) {
			WorkerConnection curr = it.next();
			if (curr.id == workerID) {
				it.remove();
				break;
			}
		}
	}
	
	public void run()	{
		while(!isStopped()) {
			try {
				Socket client = this.serverSocket.accept();
				System.out.println(client.toString());
				WorkerConnection connection = new WorkerConnection(this, client, ++id_counter);
				connection.start();
				workerQueue.add(connection);
			} catch (IOException e) {
				if(isStopped()) {
					System.err.println("Server stopped, cannot accept Workers.") ;
					return;
				}
				else 
					throw new RuntimeException("Error accepting worker connection", e);
			}
		}
        System.out.println("Master server stopped.") ;
	}
		
   public static void main(String[] args) {
	   new Master(args).start();
   }
}
