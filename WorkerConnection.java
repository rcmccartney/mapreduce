package mapreduce;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class WorkerConnection extends Thread {

    protected Socket clientSocket;
    protected final int id;
	protected InputStream in;
	protected OutputStream out;
	protected boolean stopped = false;
	protected Master master;
	protected ExecutorService outQueue;
	private byte[] byteArrOfMRFile;

    public WorkerConnection(Master master, Socket clientSocket, int id) throws IOException {
        this.clientSocket = clientSocket;
        out = clientSocket.getOutputStream();
        in = clientSocket.getInputStream();
        outQueue = Executors.newCachedThreadPool();
        this.master = master;
        this.id = id;
    }
    
    public void writeWorker(final String arg) {
    	outQueue.execute(new Runnable() {
			public void run() {
		    	try {
		    		out.write(arg.getBytes());
		    		out.flush();
		    	} catch (IOException e) {
		    		System.err.println("Error writing to Worker " + id + ": closing connection.");
		    		closeConnection();
		    	}				
			}
    	});
    }
    
    public void writeWorker(final byte... arg) {
    	outQueue.execute(new Runnable() {
			public void run() {
		    	try {
		    		out.write(arg);
		    		out.flush();
		    	} catch (IOException e) {
		    		System.err.println("Error writing to Worker " + id + ": closing connection.");
		    		closeConnection();
		    	}				
			}
    	});
    }
    
    public synchronized void closeConnection() {
    	stopped = true;
    	master.remove(id);
    	try {
    		outQueue.shutdown();
    		if (!clientSocket.isClosed()) {
    			// don't use writeWorker since that will call close recursively
    			out.write(mapreduce.Utils.MR_QUIT); 
    			out.flush();
    			clientSocket.close();
    		}
    		in.close();
    		out.close();
    		//Remove worker from filetable
    		synchronized(mapreduce.Master.fileHashTable){
	    		if (mapreduce.Master.fileHashTable.containsKey(id)){
	    			mapreduce.Master.fileHashTable.remove(id);
	    		}
    		}
    	} catch (IOException e) { }  //ignore exceptions since you are closing it anyways
    }
    
    public synchronized boolean isStopped() {
    	return stopped;
    }
    
    public String toString() {
    	return "Worker " + this.id + ": " + clientSocket.toString();
    }
    
	public void sendFile(byte[] bArr) {
		byteArrOfMRFile = bArr;
		//notify client of pending MR transmission
		writeWorker(mapreduce.Utils.MR_W);
	}

	private void receive(int command){
		
		switch(command) {
		//this workerconnection is only used for a client to send a MR job to the Master
		case mapreduce.Utils.MR_C:	
			writeWorker(mapreduce.Utils.MR_C_OKAY);
			receiveFileFromClient();
			closeConnection();
			master.sendMRFileToWorkers();
			System.out.println("finished sending and loading MR job to workers");
			break;
			
		case mapreduce.Utils.MR_W_OKAY:
			writeWorker(byteArrOfMRFile); //write the current bytearray file for the connection
			break;

		default:
			System.err.println("Invalid command received on WorkerConnection");
			break;
		}
	} 
    
	/*Function to get list of files from Worker
	 * Sends REQ_LIST(R) to worker and reads list
	 * Adds it to hashtable
	 */
	private void getFilesList(){
		try{
			out.write(Utils.REQ_LIST);
			int length = in.read();
			LinkedList<String> list = new LinkedList();
			byte[] mybytearray = new byte[1024];
			for(int i=0;i<length;i++){
				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
				if(bytesRead > 0){
					String fileName = new String(mybytearray,0,bytesRead);
					list.add(fileName);
				}
			}
			synchronized(mapreduce.Master.fileHashTable){
				mapreduce.Master.fileHashTable.put(id, list);
				System.out.println("Added for "+id+" "+list.toString());
			}
		}catch (Exception e){
			System.out.println("Error fetching file list from Worker: "+id);
		}
	}
	
    /**
     * This is the loop that listens to the socket for messages from this particular Worker
     */
    public void run() {
    	int command;
    	//Since this is first we get list of files from worker
    	getFilesList();
    	while(!isStopped()) {
    		try {
    			if ( (command = in.read()) != 0)
    				receive(command);
			} catch (IOException e) {
				if (isStopped()) // exception is expected when the connection is first closed
					return;
				System.err.println("Error in socket connection to Worker " + id + ": removing worker from cluster");
				this.closeConnection();
			}
    	}
    }
    
	private void receiveFileFromClient(){
		System.out.println("WorkerConnection: receiveFile() called");
		try{
			byte[] mybytearray = new byte[1024];
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("MR_tmp.java"));
			while(true){
				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
				System.out.println("bytes read " + bytesRead);
				if (bytesRead <= 0) break;
				bos.write(mybytearray, 0, bytesRead);
				if (bytesRead < 1024) break;
			}
			bos.close();
		} catch (IOException e) {
			System.out.println("Exception in WorkerConnection: receiveFile() " + e);
		}
	}
}