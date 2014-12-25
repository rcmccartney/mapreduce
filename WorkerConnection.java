package mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class WorkerConnection extends Thread {

    protected final int id;
    protected int workerPort;
    protected Socket clientSocket;
	protected InputStream in;
	protected OutputStream out;
	protected boolean stopped = false;
	protected Master master;
	protected String MRFileName; 

    public WorkerConnection(Master master, Socket clientSocket, int id) throws IOException {
        this.clientSocket = clientSocket;
        this.master = master;
        this.id = id;
        out = clientSocket.getOutputStream();
        in = clientSocket.getInputStream();
        // first tell the worker his ID
        out.write(id);
    }
    
     public synchronized void closeConnection() {
    	stopped = true;
    	master.remove(id);
    	try {
    		if (!clientSocket.isClosed())
    			Utils.writeCommand(out, Utils.MR_QUIT, Utils.NONE);
    		in.close();
    		out.close();
			clientSocket.close();
    	} catch (IOException e) { }  //ignore exceptions since you are closing it anyways
    }
    
    public synchronized boolean isStopped() {
    	return stopped;
    }
    
    public String toString() {
    	return "WorkerConnection " + id + ": " + clientSocket.toString();
    }
    
	public void sendFile(byte transferType, int job, String name, byte[] bArr) {
		// notify worker of pending file transmission and send data
		// two types are a regular file or a MR job 
		Utils.writeCommand(out, transferType, job);
		Utils.writeFile(out, name, bArr); 
	}

	/**
     * This is the loop that listens to the socket for messages from this particular Worker
     */
    public void run() {
        // tell the worker to send their files to you
        Utils.writeCommand(out, Utils.M2W_REQ_LIST, Utils.NONE);
    	int command;
    	while(!isStopped()) {
    		try {
    			if ((command = in.read()) != -1) {
    				// jobID is always sent next, even if it is NONE for certain requests
    				int jobID = Utils.readInt(in);
    				switch(command) {
    				case Utils.W2M_WP2P_PORT:
    					//here jobID was actually the port number to save one int 
    					this.workerPort = jobID;  
    					break;
    				case Utils.M2W_REQ_LIST_OKAY:
    					master.receiveWorkerFiles(this.id, in);
    					break;
    				case Utils.W2M_KEY:
    					master.receiveWorkerKey(this.id, in, out, jobID);
    					break;
    				case Utils.W2M_KEY_COMPLETE:
    					master.receiveKeyComplete(in, jobID);
    					break;
    				case Utils.W2M_KEYSHUFFLED:
    					master.receiveKeyShuffle(in, jobID);
    					break;
    				case Utils.W2M_RESULTS:
    					master.receiveResults(in, out, jobID);
    					break;
    				case Utils.W2M_JOBDONE:
    					master.receiveJobDone(in, jobID);
    					break;
    				case Utils.ACK:  //worker has awknowledged receiving MR job, need to send his files
    					master.receiveAck(this.id, in, out, jobID);
    					break;
    				default:
    					System.err.println("Invalid command received on WorkerConnection " + this.id + ": " + command);
    					break;
    				}
    			}
			} catch (IOException e) {
				if (isStopped()) // exception is expected when the connection is first closed
					return;
				System.err.printf("Error in socket connection to Worker " + id + ": " + e);
				this.closeConnection();
			}
    	}
    }
}