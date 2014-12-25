package mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
	private static int wkIDcounter = 0;
	private static int jobCounter = 0;
	
	protected int port = Utils.DEF_MASTER_PORT;
	protected int clientPort = Utils.DEF_CLIENT_PORT;
	protected String basePath;
	protected File baseDir;
	protected URLClassLoader myClassPathLoader;
	protected ExecutorService exec;
	protected ClientListener clientConn;
	protected ServerSocket serverSocket;
	protected boolean stopped;
	protected List<WorkerConnection> workerQueue; 
	protected Object queueLock = new Object();
	// this is the file server portion of Master
	protected Map<Integer, List<String>> IDtoFiles;
	protected Map<String, Integer> filesToID;
	// maps jobIDs to the MasterJob working them
	protected Map<Integer, MasterJob<?,?,?>> jobs;

	/**
	 * 
	 * @param args use -port X to change the server socket port to X
	 * @throws IOException if the socket connections throw an exception
	 */
	public Master(String[] args) throws IOException	{
		if (args.length > 0)
			parseArgs(args);
		stopped = false;
		// set up a classpath area for work to be done
		basePath = Utils.basePath + File.separator + "master";
    	baseDir = new File(basePath);
    	if (!baseDir.isDirectory())
    		baseDir.mkdirs();
    	myClassPathLoader = Utils.addPath(basePath);
		workerQueue = new ArrayList<>();
		filesToID = new ConcurrentHashMap<>();
		IDtoFiles = new ConcurrentHashMap<>();
		jobs = new ConcurrentHashMap<>();
		serverSocket = new ServerSocket(port);
		exec = Executors.newCachedThreadPool();
		// listen for Client connection sending a file
		clientConn = new ClientListener(this, clientPort);
		clientConn.setDaemon(true);
		clientConn.start();
	}
	
    /**
     * Asynchronously publish a byte message to all workers 
     * 
     * @param message byte message to be sent to all worker nodes
     */
    public void writeAllWorkers(final byte message){
    	synchronized(queueLock) {
	    	for (final WorkerConnection wc : workerQueue) {
	    		exec.execute(new Runnable() {
	    			public void run() {
	    				Utils.writeCommand(wc.out, message, Utils.NONE);				
	    			}
	    		});
	    	}
    	}
    }
    
    public int getJobs() {
    	return jobs.size();
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
			WorkerConnection wk = this.getWorker(Integer.parseInt(workerID));
			if (wk != null) {
				wk.sendFile(Utils.M2W_FILE, Utils.NONE, myFile.getFileName().toString(), byteArrOfFile);
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
    
    ///////////////////////////////////
    //
    // These methods are job-independent
    //
    ///////////////////////////////////
    
    protected void receiveWorkerFiles(int wkID, InputStream in) {
		List<String> wFiles = Utils.readFilenames(in);
		if (wFiles != null && wFiles.size() > 0) {
			IDtoFiles.put(wkID, wFiles);
			for(String file : wFiles) 
				filesToID.put(file, wkID);
		}
	}
    
    ///////////////////////////////////
    //
    // These methods are job-dependent
    //
    ///////////////////////////////////
	
    protected void receiveWorkerKey(int wkID, InputStream in, OutputStream out, int jobID) throws IOException {
		// Worker will send the JobID this refers to
    	jobs.get(jobID).receiveWorkerKey(in, wkID);
		out.write(Utils.ACK);  // worker waits for Ack before sending another
    }
    
    protected void receiveKeyComplete(InputStream in, int jobID) {
		// Worker will send the JobID this refers to
		jobs.get(jobID).receiveKeyComplete();
    }
    
    protected void receiveKeyShuffle(InputStream in, int jobID) {
    	// Worker will send the JobID this refers to
		jobs.get(jobID).receiveKeyShuffle();
    }
    
    protected void receiveJobDone(InputStream in, int jobID) {
    	// Worker will send the JobID this refers to
		jobs.get(jobID).receiveJobDone();
    }
    
    protected void receiveResults(InputStream in, OutputStream out, int jobID) throws IOException {
    	// Worker will send the JobID this refers to
    	jobs.get(jobID).receiveWorkerResults(in);
    	out.write(Utils.ACK);  // worker waits for Ack before sending another
    }
    
    protected void receiveAck(int wkID, InputStream in, OutputStream out, int jobID) {
    	//worker has awknowledged receiving MR job, need to send his files
		ArrayList<String> contains = new ArrayList<>();
		for(String file: jobs.get(jobID).files)  //first thing sent is jobID 
			if (filesToID.containsKey(file) && filesToID.get(file) == wkID)
				contains.add(file);
		String[] files = new String[contains.size()];
		files = contains.toArray(files);
		Utils.writeFilenames(out, files);  // worker is waiting, no need to send jobID
    }
     
    /**
     * This method sends the compiled MR job to the worker nodes
     * Since the program can be run locally or on multiple nodes, we
     * need to make sure that files are in the right place and in
     * the classpath before compiling
     * 
     * @param filename String filename to compile
     * @param filesToUse the files we want the worker nodes to use during mapping. If empty
     * 		  workers will use all the files in their local directory
     * @param local boolean on whether this file was copied from an external client
     * 		  or loaded from the command line locally
     */
	public synchronized void receiveMRJob(String filename, List<String> filesToUse, boolean local){

		//if filesToUSe is empty then use all files there
		try {
			File f1 = null, f2 = null;
			// if this file is local need to copy it to our working directory
			if (local) {
				f1 = new File(filename);
				f2 = new File(basePath + File.separator + filename);
				Utils.copyFile(f1, f2);
			}
			else //this file came from an external client 
				f2 = new File(basePath + File.separator + filename);
			// compile the file and load it into a mapper class
			String className = compile(filename);
			Class<?> myClass = myClassPathLoader.loadClass(className); 
			Mapper<?, ?, ?> mr = (Mapper<?, ?, ?>) myClass.newInstance();
			// mj gets the class information generically from Mapper
			final int currJob = ++jobCounter;
			jobs.put(currJob, new MasterJob<>(currJob, mr, this, filesToUse, workerQueue));
			// load the bytes of the compiled class and send it across the sockets to all workers
			final Path myFile = Paths.get(basePath + File.separator + className + ".class");
			final byte[] byteArrOfFile = Files.readAllBytes(myFile);
			// clean up the area 
			Files.delete(myFile);
			Files.delete(Paths.get(basePath + File.separator + f2.getName()));
			synchronized (queueLock) {
				for (final WorkerConnection wc : workerQueue) {
					exec.execute(new Runnable() {
						public void run() {
							if (!wc.isStopped()) {
								wc.sendFile(Utils.M2W_MR_UPLOAD, currJob, myFile.getFileName().toString(), byteArrOfFile);
							}
						}
					});
				}
			}
			System.out.println("Finished sending MR job to worker nodes");
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
			int compilationResult = compiler.run(null, null, null, basePath + File.separator + filename);  
			System.out.println(filename + " compilation " + (compilationResult==0?"successful":"failed"));
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
	public WorkerConnection getWorker(int id){
		// lock queue when iterating
		synchronized(queueLock) {
			for (WorkerConnection wc : workerQueue)
				if (wc.id == id)
					return wc;
		}
		return null;
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
			if (args[i].equals("-wp")) 
				port = new Integer(args[++i]).intValue();
			else if (args[i].equals("-cp")) 
				clientPort = new Integer(args[++i]).intValue();
			else {
				System.out.println("Correct usage: java Master [-wp <port>] [-cp <port>]");
				System.out.println("\t-wp: override default worker port 40001 to <port>.");
				System.out.println("\t-cp: override default client port 40000 to <port>.");
				System.exit(1);
			}
		}
	}
	
	public void remove(int workerID) {
		for(String file : IDtoFiles.get(workerID))
			filesToID.remove(file);
		IDtoFiles.remove(workerID);
		for(MasterJob<?,?,?> mj : jobs.values())
			mj.remove(workerID);
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
	
	public void run()	{
		while(!isStopped()) {
			try {
				Socket client = this.serverSocket.accept();
				WorkerConnection connection = new WorkerConnection(this, client, ++wkIDcounter);
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
