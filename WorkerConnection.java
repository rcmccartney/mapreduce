package mapreduce;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
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
	protected byte[] byteArrOfMRFile;
	protected String MRFileName; 

    public WorkerConnection(Master master, Socket clientSocket, int id) throws IOException {
        this.clientSocket = clientSocket;
        out = clientSocket.getOutputStream();
        in = clientSocket.getInputStream();
        outQueue = Executors.newCachedThreadPool();
        this.master = master;
        this.id = id;
    }
    
    public void writeWorker(final String arg, final byte... barg) {
    	outQueue.execute(new Runnable() {
			public void run() {
				try {
					byte[] barr = Utils.concat(arg.getBytes(), barg);
					out.write(barr);
					out.flush();
				} catch (IOException e) {
					System.err.printf("Error writing to Worker %d: closing connection%n", id);
					closeConnection();
				}
			}
    	});
    }
    
    public void writeWorker(final String arg) {
    	outQueue.execute(new Runnable() {
			public void run() {
		    	try {
		    		out.write(arg.getBytes());
		    		out.flush();
		    	} catch (IOException e) {
		    		System.err.printf("Error writing to Worker %d: closing connection%n", id);
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
		    		System.err.printf("Error writing to Worker %d: closing connection%n", id);
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
    	} catch (IOException e) { }  //ignore exceptions since you are closing it anyways
    }
    
    public synchronized boolean isStopped() {
    	return stopped;
    }
    
    public String toString() {
    	return "Worker " + this.id + ": " + clientSocket.toString();
    }
    
    private void receive(int command){
		
		switch(command) {
		//this workerconnection is only used for a client to send a MR job to the Master
		// after which it is shutdown
		case Utils.C2M_UPLOAD:	
			writeWorker(mapreduce.Utils.C2M_UPLOAD_OKAY);
			String name = receiveFileFromClient();
			closeConnection();
			if (name != null) {
				master.sendMRFileToWorkers(name, true);
				System.err.println("...Finished sending MR job to worker nodes");
			}
			break;
		// this is called when the Worker notifies a connection it is ready to receive file
		case Utils.M2W_UPLOAD_OKAY:
			writeWorker(MRFileName+'\n', byteArrOfMRFile);  //newline critical
			break;
		case Utils.W2M_KEY:
			writeWorker(Utils.W2M_KEY_OKAY);
			receiveWorkerKeys();
			break;
		case Utils.M2W_KEYASSIGN_OKAY:
			// TODO send the Worker the keys he's assigned
			break;
		case Utils.W2M_RESULTS:
			writeWorker(Utils.W2M_RESULTS_OKAY);
			// TODO receive the results from the worker after Job is finished
			break;
		default:
			System.err.println("Invalid command received on WorkerConnection");
			break;
		}
	} 
    
    public void sendFile(String name, byte[] bArr) {
		MRFileName = name;
		byteArrOfMRFile = bArr;
		//notify client of pending MR transmission and wait for response
		writeWorker(mapreduce.Utils.M2W_UPLOAD);
	}
    
	private String receiveFileFromClient(){
		System.err.println("...Receiving MR job from Client node");
		try{
			// the first thing sent will be the filename
			int f;
			String name = "";
			while( (f = in.read()) != '\n') {
				name += (char) f;
			}
			int totalBytes = 0;
			byte[] mybytearray = new byte[1024];
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(name));
			while (true){
				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
				totalBytes += bytesRead;
				if (bytesRead <= 0) break;
				bos.write(mybytearray, 0, bytesRead);
				if (bytesRead < 1024) break;
			}
			System.err.printf("%d bytes read%n", totalBytes);
			bos.close();
			return name;
		} catch (IOException e) {
			System.err.println("Exception in WorkerConnection: receiveFile() " + e);
			return null;
		}
	}
    
    private void receiveWorkerKeys() {
		// TODO receive this workers keys and give to Master to compile
	}

	/**
     * This is the loop that listens to the socket for messages from this particular Worker
     */
    public void run() {
    	int command;
    	while(!isStopped()) {
    		try {
    			if ( (command = in.read()) != 0)
    				receive(command);
			} catch (IOException e) {
				if (isStopped()) // exception is expected when the connection is first closed
					return;
				System.err.printf("Error in socket connection to Worker %d: removing worker from cluster%n", id);
				this.closeConnection();
			}
    	}
    }
}