package mapreduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Scanner;


public class Master extends Thread {

	protected ServerSocket serverSocket;
	protected int port = 40001;
	protected boolean stopped = false;
	protected int jobs = 0;
	protected static int id_counter = 0;
	protected Collection<WorkerConnection> workerQueue; 
	
	public Master(String[] args) throws IOException	{
		workerQueue = new ArrayList<>();
		if (args.length > 0)
			parseArgs(args);
		serverSocket = new ServerSocket(port);
	}
	
	public void receive(String command, int id) {
		// TODO Auto-generated method stub
	}
	
    private synchronized boolean isStopped() {
        return stopped;
    }
    
    public synchronized void writeAllWorkers(String message){
    	Iterator<WorkerConnection> it = workerQueue.iterator();
		while (it.hasNext())
			it.next().writeWorker(message);
    }
    
    private synchronized int getJobs() {
    	return jobs;
    }
    
    public Iterator<WorkerConnection> iterator() {
    	return workerQueue.iterator();
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
	protected void parseArgs(String args[]) {
		
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
				WorkerConnection connection = new WorkerConnection(this, client, ++id_counter);
				connection.setDaemon(true);  // this will cause exit upon user 'quit'
				connection.start();
				synchronized (this) {  // make this synchronized to prevent modification while iterating
					workerQueue.add(connection);
				}
			} catch (IOException e) {
				if(isStopped()) {
					System.err.println("Master server stopped.") ;
					return;
				}
				else 
					throw new RuntimeException("Error accepting worker connection", e);
			}
		}
        System.out.println("Master server stopped.") ;
	}
	
	////////////////////////////////////////////////////////////////////////////////////
	//  Command-line interface services 
	////////////////////////////////////////////////////////////////////////////////////
	/**
	 * This is how a user interacts with the Master node of the system.  
	 * It is run by the main thread of execution
	 * @throws UnknownHostException 
	 * 
	 */
	protected void commandLineInterface() throws UnknownHostException {
		System.out.println("#################################################");
		System.out.println("#\t\tMAP-REDUCE FRAMEWORK\t\t#");
		System.out.println("#\t\t\t\t\t\t#");
		System.out.println("# Server:"+InetAddress.getLocalHost().getHostAddress()+"\t\t\t\t#");
		System.out.println("# Port:"+port+"\t\t\t\t\t#");	
		System.out.println("# Type help or man to view the man pages\t#");
		System.out.println("#\t\t\t\t\t\t#");
		System.out.println("#################################################");
		Scanner in = new Scanner(System.in);
		do {
			System.out.print("> ");
			String command;
			command = in.nextLine().trim();
			String[] line = command.split("\\s+");
			if (line[0].equalsIgnoreCase("man") || 
					(line[0].equalsIgnoreCase("help") && line.length==1))
				printFull();
			else if (line[0].equalsIgnoreCase("help"))
				if (line[1].equalsIgnoreCase("ls"))
					printLS();
				else if (line[1].equalsIgnoreCase("man"))
					printMan();
				else if (line[1].equalsIgnoreCase("q"))
					printQ();
				else if (line[1].equalsIgnoreCase("help"))
					printHelp();
				else
					unrecognized(line[1]);
			else if (line[0].equalsIgnoreCase("ls")) {
				synchronized (this) {
					for (WorkerConnection wc : workerQueue)
						System.out.println(wc);
				}
			}
			else if (line[0].equalsIgnoreCase("q")) {
				System.out.printf("Really quit? There %s %d job%s pending%n> ", 
						(getJobs()==1?"is":"are"), getJobs(), (getJobs()==1?"":"s"));
				command = in.nextLine().trim();
				if (command.equalsIgnoreCase("y") || command.equalsIgnoreCase("yes")) 
					stopServer();
			}
			else 
				unrecognized(line[0]);
		} while (!isStopped());
		in.close();
	}
	
	protected void unrecognized(String cmd) {
		if (cmd.length() > 0)
			System.out.println(cmd + " is not recognized as a valid input command");
	}
	
	protected void printFull() {
		printMan();
		printHelp();
		printLS();
		printQ();
	}
	
	protected void printHelp() {
		System.out.println("help <cmd>: Get further information on <cmd>");
	}
	
	protected void printQ() {
		System.out.println("q: Quit the system (y to confirm)");
	}
	
	protected void printMan() {
		System.out.println("man: Display manual");
	}
	
	protected void printLS() {
		System.out.println("ls: List the workers currently in the cluster");
	}
		
	public static void main(String[] args) throws IOException {
		Master m = new Master(args);
		m.start();
		m.commandLineInterface();  //run by main thread of execution
	}
}
