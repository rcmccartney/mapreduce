package mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

public class Master extends Thread {

	protected MasterJob<?, ?> mj = null;
	protected ServerSocket serverSocket;
	protected int port = Utils.DEF_MASTER_PORT;
	protected boolean stopped = false;
	protected int jobs = 0;
	protected static int id_counter = 0;
	protected List<WorkerConnection> workerQueue; 
	// Hashtable is synchronized 
	private Hashtable<Integer, List<String>> fileHashTable;
	// stores map from Worker to Port for W2W comms
	protected Map<Integer, Integer> workerIDAndPorts;
	protected String MRFileName;
	protected ClientListener clients;
	
	public Master(String[] args) throws IOException	{
		workerQueue = new ArrayList<>();
		fileHashTable = new Hashtable<>();
		workerIDAndPorts = new HashMap<>();
		if (args.length > 0)
			parseArgs(args);
		serverSocket = new ServerSocket(port);
		clients = new ClientListener(this);
		clients.setDaemon(true);
		clients.start();
	}
	
    private synchronized boolean isStopped() {
        return stopped;
    }
    
    public synchronized void writeAllWorkers(byte... message){
    	for (WorkerConnection wc : workerQueue)
    		wc.writeWorker(message);
    }
    
     public void sendRegularFile(String workerID, String filename) {
		byte[] byteArrOfFile = null;
		try {
			Path myFile = Paths.get(filename);
			byteArrOfFile = Files.readAllBytes(myFile);
			WorkerConnection wk = this.getWCwithId(Integer.parseInt(workerID));
			if (wk != null) {
				wk.sendFile(myFile.getFileName().toString(), 
							byteArrOfFile, Utils.M2W_FILE);
				System.out.printf("%s sent to Worker %s%n", filename, workerID);
			}
			else 
				System.err.printf("%s is not in the cluser%n", workerID);
		} catch (NumberFormatException n) {
			System.err.println("Not a valid worker ID");
		}
		catch (IOException e) {
			System.err.println("Error reading file in Master node");
		}
    }
     
 	/*Function to get list of files from Worker
 	 * Sends REQ_LIST(R) to worker and reads list
 	 * Adds it to hashtable
 	 */
 	public List<String> getFilesList(InputStream in) {
 		try {
 			int length = in.read();  // TODO won't work for more than 1 byte of files
 			List<String> list = new LinkedList<>();
 			byte[] mybytearray = new byte[1024];
 			for(int i=0;i<length;i++) {
 				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
 				if(bytesRead > 0) {
 					String fileName = new String(mybytearray,0,bytesRead);
 					list.add(fileName);
 				}
 			}
 			return list;
 		} catch (IOException e) {
 			System.err.println("Exception while receiving file listing from Client: " + e);
 			return null;
 		}
 	}
    
	// check if this method needs to be called by multiple threads. 
    // i.e multiple clients trying to submit MRFiles
    // this method is for sending file received by a client
    // use deleteAfter if this is a temporary file sent from client to Master
	public void setMRJob(String filename, List<String> filesToUse, boolean deleteAfter){

		//if filesToUSe is empty then use all files there
		byte[] byteArrOfFile = null;
		try {
			// TODO when jobs is already > 0 store this job for later
			if (jobs == 0) {
				File f1 = null, f2 = null;
				if (!deleteAfter){
					f1 = new File(filename);
					f2 = new File(f1.getName());
					copyFile(f1, f2);
				}
				else { //this file came from an external client
					f2 = new File(filename);
				}
				String className = compile(f2.getName());
				Class<?> myClass = ClassLoader.getSystemClassLoader().loadClass(className); 
				Mapper<?, ?> mr = (Mapper<?, ?>) myClass.newInstance();
				mj = new MasterJob<>(mr,this);
				Path myFile = Paths.get(className + ".class");
				byteArrOfFile = Files.readAllBytes(myFile);
				Files.delete(Paths.get(className + ".class"));
				Files.delete(Paths.get(f2.getName()));
				synchronized (this) {
					for (WorkerConnection wc : workerQueue)
						if(!wc.isStopped())
							wc.sendFile(myFile.getFileName().toString(), byteArrOfFile, Utils.M2W_MR_UPLOAD);
				}
				System.err.println("...Finished sending MR job to worker nodes");
			}
		} catch (IOException e) { 
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
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
	
	protected String compile(String filename){
		try {	
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();  
			if (compiler == null)  // needs to be a JDK java.exe to have a compiler attached
				throw new RuntimeException("Error: no compiler set for MR file");
			int compilationResult = compiler.run(null, null, null, filename);  
			System.err.println(filename + " compilation " + (compilationResult==0?"successful":"failed"));
			return filename.split("\\.")[0];  // class name is before ".java"
		} catch (Exception e) {
			System.err.println("Exception loading or compiling the File: " + e);
			return null;
		}
	} 			
	
	public synchronized WorkerConnection getWCwithId(int id){
		// lock queue when iterating, assumes other threads lock on workerQueue too before using it
		for (WorkerConnection wc : workerQueue)
			if (wc.id == id)
				return wc;
		return null;
	}
    
    private synchronized int getJobs() {
    	return jobs;
    }
    
    private void printFiles(int workerID) {
    	List<String> l = fileHashTable.get(workerID);
    	for (String file : l) 
    		System.out.println("  " + file);
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
	
	public synchronized void remove(int workerID) {
		
		fileHashTable.remove(workerID);
		Iterator<WorkerConnection> it = workerQueue.iterator();
		while (it.hasNext()) {
			WorkerConnection curr = it.next();
			if (curr.id == workerID) {
				it.remove();
				break;
			}
		}
	}
	
	//TODO better synchronization instead of hastable using concurrent Hashmap?
	public void addFiles(Integer workerID, List<String> files) {
		fileHashTable.put(workerID, files);
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
					System.err.println("Master server stopped") ;
					return;
				}
				else 
					throw new RuntimeException("Error accepting worker connection", e);
			}
		}
        System.out.println("Master server stopped") ;
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
				else if (line[1].equalsIgnoreCase("ld"))
					printLD();
				else if (line[1].equalsIgnoreCase("lf"))
					printLF();
				else
					unrecognized(line[1]);
			else if (line[0].equalsIgnoreCase("ls")) {
				synchronized (this) {
					for (WorkerConnection wc : workerQueue) {
						System.out.println(wc);
						if (line.length > 1 && line[1].equals("-l"))
							printFiles(wc.id);
					}
				}
			}
			else if (line[0].equalsIgnoreCase("q")) {
				System.out.printf("Really quit? There %s %d job%s pending%n> ", 
						(getJobs()==1?"is":"are"), getJobs(), (getJobs()==1?"":"s"));
				command = in.nextLine().trim();
				if (command.equalsIgnoreCase("y") || command.equalsIgnoreCase("yes")) 
					stopServer();
			}
			else if (line[0].equalsIgnoreCase("ld")) {
				if (line.length > 1) {
					List<String> filesToUse = new LinkedList<>();
					for(int i = 2; i < line.length; i++)
						filesToUse.add(line[i]);
					setMRJob(line[1], filesToUse, false);
				}
				else {
					System.out.printf("Enter filename (operates on all worker files):%n> ");
					command = in.nextLine().trim();
					setMRJob(command, new LinkedList<String>(), false);
				}
			}
			else if (line[0].equalsIgnoreCase("lf")) {
				if (line.length == 3) {
					sendRegularFile(line[1], line[2]);
				}
				else {
					System.out.printf("Enter worker ID:%n> ");
					String wk = in.nextLine().trim();
					System.out.printf("Enter filename:%n> ");
					command = in.nextLine().trim();
					sendRegularFile(wk, command);
				}
			}
			else if (line[0].equalsIgnoreCase("worker")) { //temp code, just to test WP2P communication
				try {
					new WorkerP2P(40013, null).send("Kumar", 
							Arrays.asList("A", "B", "D"), "127.0.0.1", Utils.BASE_WP2P_PORT);
				} catch (IOException e) {
					e.printStackTrace();
				}
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
		printLD();
		printLF();
		printQ();
	}
	
	protected void printHelp() {
		System.out.println("help <cmd>: get further information on <cmd>");
	}
	
	protected void printQ() {
		System.out.println("q: quit the system (y to confirm)");
	}
	
	protected void printMan() {
		System.out.println("man: display manual");
	}
	
	protected void printLS() {
		System.out.println("ls [-l]: list the workers currently in the cluster");
		System.out.println(" -l includes the files located at each worker");
	}
	
	protected void printLD() {
		System.out.println("ld [filename]: load the map-reduce job");
	}
	
	protected void printLF() {
		System.out.println("lf [workerID] [filename]: load the file to Worker workerID");
	}
		
	public static void main(String[] args) throws IOException {
		Master m = new Master(args);
		m.start();
		m.commandLineInterface();  //run by main thread of execution
	}
}
