package mapreduce;
//******************************************************************************
//File:    Worker.java
//Package: None
//Unit:    Distributed Programming Group Project
//******************************************************************************

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class parses command-line input in order to register client as a worker in a 
 * MapReduce architecture.  Once connected, the nodes send heartbeats to each other 
 * as a connected distributed system.  This node will then wait to be sent jobs 
 * by the Master node.  
 * 
 * 
 * @author rob mccartney
 */
public class Worker extends SocketClient implements Runnable {

	//stopped is used by multiple threads, must be synchronized
	protected boolean stopped = false;
	protected WorkerP2P wP2P;
	protected String basePath;
	protected File baseDir;
	protected URLClassLoader myClasspathLoader;
	protected Map<Integer, Job<?,?,?>> jobs;
    
    /**
     * Constructor that makes a new worker and attempts to register with a Master.
     * Master node must be already running
     * 
     * @param args String[] from the command line
     */
    public Worker(String[] args) {
    	super(args);
    	try {
			wP2P = new WorkerP2P(Utils.BASE_WP2P_PORT+id, this);
		} catch (IOException e) {
			System.err.println("Cannot open P2P socket: " + e);
			this.closeConnection();
			System.exit(1);
		} 
    	// inform Master of your P2P port number, send it in place of a jobID
    	Utils.writeCommand(out, Utils.W2M_WP2P_PORT, Utils.BASE_WP2P_PORT+id);
    	basePath = Utils.basePath + File.separator + id;
    	baseDir = new File(basePath);
    	if (!baseDir.isDirectory())
    		baseDir.mkdirs();
    	myClasspathLoader = addPath(basePath);
    	jobs = new ConcurrentHashMap<>();
    	new Thread(this).start();  //start a thread to read from the Master
    }
 
    @Override
    public void usageTemplate() {
		System.out.println("Correct usage: java Worker [-h <hostName>] [-p <portnumber>]");
		System.out.println("\t-h: override localhost to set the host to <hostName>.");
		System.out.println("\t-p: override default port 40001 to <port>.");
		System.out.println("\t<host> and <port> must match the Master Server's worker connection.");
		System.exit(1);
    }
    
    public synchronized boolean isStopped() {
    	return stopped;
    }
    
    public String toString() {
    	return "Worker " + id + ": " + socket.toString();
    }

    public synchronized void closeConnection() {
    	super.closeConnection();
    	stopped = true;
    	for (Job<?,?,?> job : jobs.values()) 
    		job.stopExecution();
    	try {
        	Files.deleteIfExists(Paths.get(basePath));
    		wP2P.closeConnection();
    	} catch (IOException e) {} //ignore exceptions since you are quitting
    }
    
    // need to do add path to Classpath with reflection since the URLClassLoader.addURL(URL url) method is protected
    // this allows each worker to have their own private folder if run on the same machine
    public static URLClassLoader addPath(String s) {
        try {
        	File f = new File(s);
	        URI u = f.toURI();
	        URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
	        Class<URLClassLoader> urlClass = URLClassLoader.class;
	        Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
	        method.setAccessible(true);
	        method.invoke(urlClassLoader, new Object[]{u.toURL()});
	        return urlClassLoader;
        } catch (Exception e) {
        	System.err.println("Exception in adding Worker classpath: " + e);
        	return null;
        }
    }
    
    private Mapper<?, ?, ?> loadMRFile(String classfile) {		
		try {	
			// need each worker to have its own directory in case it is running locally
			// this requires the classpath to be changed
			Class<?> myClass = myClasspathLoader.loadClass(classfile.split("\\.")[0]); 
			Mapper<?, ?, ?> mr = (Mapper<?, ?, ?>) myClass.newInstance();
			// clean up the files you created
			Files.delete(Paths.get(basePath + File.separator + classfile));
			return mr;
		} catch (Exception e) {
			System.err.println("Exception loading or compiling the File: " + e);
			return null;
		}
	}
	
	/* Function to send filesList to Master
     * Path of default directory is in Utils
     */
    public void sendFilesList(File path) {
    	if (path.isDirectory()) {
    		File[] filesList = path.listFiles();
    		String[] names = new String[filesList.length];
    		for(int i=0; i < filesList.length; i++)
    			names[i] = filesList[i].getName();
    		Utils.writeCommand(out, Utils.M2W_REQ_LIST_OKAY, Utils.NONE);
        	Utils.writeFilenames(out, names);
    	}
    	else {
    		Utils.writeCommand(out, Utils.M2W_REQ_LIST_OKAY, Utils.NONE);
    		Utils.writeFilenames(out, new String[0]);
    	}
    }
    
    protected void receiveNewJob(int jobID) throws IOException {
    	System.out.print("Worker received new MR job: ");
		Mapper<?, ?, ?> mr = loadMRFile(Utils.receiveFile(in, basePath + File.separator));
		if (mr != null) {
			Utils.writeCommand(out, Utils.ACK, jobID);  // notify master, next message received is the file listing
			List<String> names = Utils.readFilenames(in);
			if (names.size() == 0)  // Master sent nothing, use all local files
				names = new ArrayList<String>(Arrays.asList(baseDir.list()));
			Job<?,?,?> currentJob = new Job<>(jobID, this, mr, names);
			jobs.put(jobID, currentJob);
			currentJob.begin(basePath);
		}
    }
    
    public void run() {
    	int command;
    	while(!isStopped()) {
    		try {
    			if ((command = in.read()) != -1) {
    				int jobID = Utils.readInt(in);
    				switch(command) {
    				case Utils.MR_QUIT:  //quit command
    		    		this.closeConnection();
    		    		break;
    				case Utils.M2W_COORD_KEYS:	
    					jobs.get(jobID).receiveKeyAssignments();
    					break;
    				case Utils.M2W_BEGIN_REDUCE:
    					jobs.get(jobID).reduce();
    					break;	
    				case Utils.M2W_MR_UPLOAD:
    					receiveNewJob(jobID);
    					break;
    				case Utils.M2W_FILE:
    					Utils.receiveFile(in, basePath + File.separator);
    					break;
    				case Utils.M2W_REQ_LIST:  // master is requesting file list
    					sendFilesList(baseDir);
    					break;
    				default:
    					System.err.println("Unrecognized Worker command: " + command);
    					break;
    				}    				
    			}
			} catch (IOException e) {
				if (isStopped()) // exception is expected when the connection is first closed
					return;
				System.err.println("Error in socket connection to Master: " + e);
				this.closeConnection();
			}
    	}
    }
    
	/**
	 * @param args for hostname or port to not be default
	 */
	public static void main(String[] args) {
		new Worker(args);
	}
}

