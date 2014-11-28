package mapreduce;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkerConnection extends Thread {

    protected Socket clientSocket;
    protected final int id;
	protected BufferedReader in;
	protected OutputStream out;
	protected boolean stopped = false;
	protected Master master;
	protected ExecutorService outQueue;
	protected InputStream inStream;
	private byte[] byteArrOfMRFile;

	public void setFileByteArr(byte[] bArr) {
		byteArrOfMRFile = bArr;
	}

    public WorkerConnection(Master master, Socket clientSocket, int id) throws IOException {
        this.clientSocket = clientSocket;
        out = clientSocket.getOutputStream();
        inStream = clientSocket.getInputStream();
        in = new BufferedReader(new InputStreamReader(inStream));
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
    
    public void writeWorker(final byte[] arg) {
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
    			out.write("q".getBytes()); 
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
    
    @Override
    public String toString() {
    	return "Worker " + this.id + ": " + clientSocket.toString();
    }

	private void receive(String command){
		switch(command) {
		case other.Utils.MR_C:	
			writeWorker(other.Utils.MR_C_OKAY+"\n");
			receiveFileFromClient();
			this.closeConnection();
			master.sendMRFileToWorkers();
			System.out.println("finished sending and loading MR job to workers");
			break;
			
		case other.Utils.MR_W_OKAY:
			writeWorker(byteArrOfMRFile); //write the current bytearray file for the connection
			break;

		default:
			System.err.println("Invalid command received on WorkerConnection");
			break;
		}
	} 
    
    /**
     * This is the loop that listens to the socket for messages from this particular Worker
     */
    public void run() {
    	String command;
    	while(!isStopped()) {
    		try {
    			if ( (command = in.readLine()) != null)
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
				int bytesRead = inStream.read(mybytearray, 0, mybytearray.length);
				System.out.println("bytes read " + bytesRead);
				if (bytesRead <= 0) break;
				bos.write(mybytearray, 0, bytesRead);
				if (bytesRead < 1024) break;
			}
			bos.close();
		} catch (IOException e) {
			System.out.println("Exception in WorkerConnection: receiveFile() " + e);
			System.out.println("Exception in WorkerConnection: receiveFile() " + e);
		}
	}
}