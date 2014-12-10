package mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/**
 * This class is the Master node of the cluster, and extends Thread to 
 * run a server socket listening for worker connections.
 * 
 * Usage: java mapreduce.Master [-port <portNumber>]
 * 
 * @version 12/7/2014
 * @author Rob McCartney
 */
public class Master extends Thread {

	//used to number the workers
	private static int wkID = 0;
	
	protected int port = Utils.DEF_MASTER_PORT;
	protected MasterJob<?, ?> mj;
	protected ExecutorService exec;
	protected ClientListener clientConn;
	protected ServerSocket serverSocket;
	protected boolean stopped;
	protected int jobs;
	protected List<WorkerConnection> workerQueue; 
	protected Object queueLock = new Object();
	// Hashtable is synchronized for multiple worker sends
	// stores map from Worker to Port for W2W comms
	protected Map<Integer, Integer> workerIDToPort;
	// this is the file server portion of Master
	protected Map<Integer, List<String>> IDtoFiles;
	protected Map<String, Integer> filesToID;
	
	/**
	 * 
	 * @param args use -port X to change the server socket port to X
	 * @throws IOException if the socket connections throw an exception
	 */
	public Master(String[] args) throws IOException	{
		if (args.length > 0)
			parseArgs(args);
		stopped = false;
		jobs = 0;
		workerQueue = new ArrayList<>();
		filesToID = new Hashtable<>();
		IDtoFiles = new Hashtable<>();
		workerIDToPort = new Hashtable<>();
		serverSocket = new ServerSocket(port);
		exec = Executors.newCachedThreadPool();
		// listen for Client connection sending a file
		clientConn = new ClientListener(this);
		clientConn.setDaemon(true);
		clientConn.start();
	}
	
	/**
	 * Use this method as thread-safe access into stopped 
	 * 
	 * @return boolean if this socket is stoppped or not 
	 */
    protected synchronized boolean isStopped() {
        return stopped;
    }
    
    /**
     * Asynchronously publish a byte message to all workers in the cluster
     * 
     * @param message byte message to be sent to all worker nodes
     */
    public void writeAllWorkers(final byte message){
    	synchronized(queueLock) {
	    	for (final WorkerConnection wc : workerQueue) {
	    		exec.execute(new Runnable() {
	    			public void run() {
	    				Utils.write(wc.out, message);				
	    			}
	    		});
	    	}
    	}
    }
    
    /**
     * Used by the user on the command line to send a file to a worker as a 
     * stream of bytes.
     * 
     * @param workerID unique ID of worker to send the file to
     * @param filename the String name of the file being sent
     */
    public void sendRegularFile(String filename, String workerID) {
		try {
			Path myFile = Paths.get(filename);
			byte[] byteArrOfFile = Files.readAllBytes(myFile);
			WorkerConnection wk = this.getWCwithId(Integer.parseInt(workerID));
			if (wk != null) {
				wk.sendFile(Utils.M2W_FILE, myFile.getFileName().toString(), byteArrOfFile);
				System.out.printf("%s sent to Worker %s%n", filename, workerID);
			}
			else 
				System.err.printf("%s is not in the cluser%n", workerID);
		} catch (NumberFormatException n) {
			System.err.println("Not a valid worker ID: " + n);
		}
		catch (IOException e) {
			System.err.println("Error reading file in Master node: " + e);
		}
    }
    
    protected void receive(InputStream in, OutputStream out, int wkID, int command) {
		
		switch(command) {
		case Utils.W2M_WP2P_PORT:
			workerIDToPort.put(wkID, Utils.readInt(in));
			break;
		case Utils.M2W_REQ_LIST_OKAY:
			List<String> wFiles = Utils.getFilesList(in);
			if (wFiles != null && wFiles.size() > 0)
				this.addFiles(wkID, wFiles);
			break;
		case Utils.W2M_KEY:
			mj.receiveWorkerKey(in, wkID);
			Utils.write(out, Utils.AWK);  // worker waits for Awk before sending again
			break;
		case Utils.W2M_KEY_COMPLETE:
			//TODO: change wCount to compare with current actual # of workers on this job 
			//		with a valid timeout
			mj.wCount++;
			if (mj.wCount == workerQueue.size()) // master now has all the keys from the workers
				mj.coordinateKeysOnWorkers();
			break;
		case Utils.W2M_KEYSHUFFLED:
			mj.wShuffleCount++;
			if(mj.wShuffleCount == workerQueue.size()) {
				System.err.println("...Shuffle & sort completed: starting reduction");
				writeAllWorkers(Utils.M2W_BEGIN_REDUCE);
			}
			break;
		case Utils.W2M_RESULTS:
			mj.receiveWorkerResults(in);
			Utils.write(out, Utils.AWK);
			break;
		case Utils.W2M_JOBDONE:
			mj.wDones++;
			if (mj.wDones == workerQueue.size())
				mj.printResults();
			break;
		case Utils.AWK_MR:  //worker has awknowledged receiving MR job, need to send his files
			ArrayList<String> contains = new ArrayList<>();
			for(String file: mj.files) 
				if (filesToID.containsKey(file) && filesToID.get(file) == wkID)
					contains.add(file);
			String[] files = new String[contains.size()];
			files = contains.toArray(files);
			Utils.write(out, files);
			break;
		default:
			System.err.println("Invalid command received on WorkerConnection " + wkID + ": " + command);
			break;
		}
	} 
     
    /**
     * This method sends the compiled MR job to the worker nodes
     * Since the program can be run locally or on multiple nodes, we
     * need to make sure that files are in the right place and in
     * the classpath before compiling
     * 
     * TODO - multiple jobs in a queue using wait/notify
     * 
     * @param filename String filename to compile
     * @param filesToUse the files we want the worker nodes to use during mapping. If empty
     * 		  workers will use all the files in their local directory
     * @param local boolean on whether this file was copied from an external client
     * 		  or loaded from the command line locally
     */
	public void setMRJob(String filename, List<String> filesToUse, boolean local){

		//if filesToUSe is empty then use all files there
		try {
			if (jobs == 0) {
				File f1 = null, f2 = null;
				// if deleteAfter is false then this file is local, need to copy it
				// to our working directory
				if (local){
					f1 = new File(filename);
					f2 = new File(f1.getName());
					Utils.copyFile(f1, f2);
				}
				else //this file came from an external client 
					f2 = new File(filename);
				// compile the file and load it into a mapper class
				String className = compile(f2.getName());
				Class<?> myClass = ClassLoader.getSystemClassLoader().loadClass(className); 
				Mapper<?, ?> mr = (Mapper<?, ?>) myClass.newInstance();
				// mj gets the class information generically from Mapper
				mj = new MasterJob<>(mr, this, filesToUse);
				// load the bytes of the compiled class and send it across the sockets to all workers
				final Path myFile = Paths.get(className + ".class");
				final byte[] byteArrOfFile = Files.readAllBytes(myFile);
				// TODO change master classpath so client can run locally w/o deleting files
				//Files.delete(Paths.get(className + ".class"));
				//Files.delete(Paths.get(f2.getName()));
				synchronized (queueLock) {
					for (final WorkerConnection wc : workerQueue) {
						exec.execute(new Runnable() {
							public void run() {
								if (!wc.isStopped())
									wc.sendFile(Utils.M2W_MR_UPLOAD, myFile.getFileName().toString(), byteArrOfFile);		
							}
						});
					}
				}
				System.out.println("...Finished sending MR job to worker nodes");
			}
			else {
				//TODO >0 jobs
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
	
	/**
	 * Compiles a filename into a java class and returns the String classname
	 * 
	 * @param filename String name of the file to be compiled, which has been copied already
	 * 		  to the correct classpath location
	 * @return String name of the class that was compiled
	 * @throws RuntimeException if you are not using the JDK java executable instead of the jre,
	 * 		   since it has a system compiler attached
	 */
	protected String compile(String filename){
		try {	
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();  
			if (compiler == null)  // needs to be a JDK java.exe to have a compiler attached
				throw new RuntimeException("Error: no compiler set for MR file");
			int compilationResult = compiler.run(null, null, null, filename);  
			System.out.println("..." + filename + " compilation " + (compilationResult==0?"successful":"failed"));
			return filename.split("\\.")[0];  // class name is before ".java"
		} catch (Exception e) {
			System.err.println("Exception loading or compiling the File: " + e);
			return null;
		}
	} 			
	
	/**
	 * Helper function to find a Worker with a given ID.  
	 * O(N) - can change to a heap for O(1) but the number of 
	 * workers is tiny compared to the amount of computation so 
	 * this might not be noticeable
	 * 
	 * @param id int that you want to convert to its owning Worker
	 * @return WorkerConnection that has this unique ID
	 */
	public WorkerConnection getWCwithId(int id){
		// lock queue when iterating
		synchronized(queueLock) {
			for (WorkerConnection wc : workerQueue)
				if (wc.id == id)
					return wc;
		}
		return null;
	}
    
	/**
	 * Helper method for multi-threaded access to jobs
	 * 
	 * @return number of jobs
	 */
    protected synchronized int getJobs() {
    	return jobs;
    }
    
    public void stopServer() {
        // need to synchronize before touching stopped, a multi-threaded variable
    	synchronized(this) {
        	this.stopped = true;
        }
        try {
            this.serverSocket.close();
            this.clientConn.closeConnection();
            this.exec.shutdown();
            synchronized(queueLock) {
            	for (WorkerConnection conn : workerQueue)
            		conn.closeConnection();
            }
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
				System.out.println("Correct usage: java Master [-port <portnumber>]");
				System.out.println("\t-port: override default port 40001 to <port>.");
				System.exit(1);
			}
		}
	}
	
	public void remove(int workerID) {
		for(String file : IDtoFiles.get(workerID))
			filesToID.remove(file);
		IDtoFiles.remove(workerID);
		synchronized(queueLock) {
			Iterator<WorkerConnection> it = workerQueue.iterator();
			while (it.hasNext()) {
				WorkerConnection curr = it.next();
				if (curr.id == workerID) {
					it.remove();
					break;
				}
			}
		}
	}
	
	//TODO better synchronization instead of hastable using concurrent Hashmap?
	public void addFiles(Integer workerID, List<String> files) {
		IDtoFiles.put(workerID, files);
		for(String file : files) 
			filesToID.put(file, workerID);
	}
	
	public void run()	{
		while(!isStopped()) {
			try {
				Socket client = this.serverSocket.accept();
				WorkerConnection connection = new WorkerConnection(this, client, ++wkID);
				connection.setDaemon(true);  // this will cause exit upon user 'quit'
				connection.start();
				synchronized (queueLock) {  // make this synchronized to prevent modification while iterating
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
	

		
	public static void main(String[] args) throws IOException {
		Master m = new Master(args);
		m.start();
		new CommandLine(m).start();  //run by main thread of execution
	}
}
