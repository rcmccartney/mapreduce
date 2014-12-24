package mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class WorkerConnection extends Thread {

    protected Socket clientSocket;
    protected final int id;
	protected InputStream in;
	protected OutputStream out;
	protected boolean stopped = false;
	protected Master master;
	protected byte[] byteArrOfMRFile;
	protected String MRFileName; 

    public WorkerConnection(Master master, Socket clientSocket, int id) throws IOException {
        this.clientSocket = clientSocket;
        out = clientSocket.getOutputStream();
        in = clientSocket.getInputStream();
        this.master = master;
        this.id = id;
        // first tell the worker his ID
        out.write(id);
    }
    
     public synchronized void closeConnection() {
    	stopped = true;
    	master.remove(id);
    	try {
    		if (!clientSocket.isClosed()) {
    			// don't use writeWorker since that will call close recursively
    			out.write(Utils.MR_QUIT); 
    			out.flush();
    		}
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
    
	public void sendFile(byte transferType, String name, byte[] bArr) {
		// notify worker of pending file transmission and send data
		// two types are a regular file or a MR job 
		Utils.writeFile(out, transferType, name, bArr); 
	}

	/**
     * This is the loop that listens to the socket for messages from this particular Worker
     */
    public void run() {
        // tell the worker to send their files to you
        Utils.writeCommand(out, Utils.M2W_REQ_LIST);
    	int command;
    	while(!isStopped()) {
    		try {
    			if ((command = in.read()) != -1)
    				master.receive(in, out, this.id, command);
			} catch (IOException e) {
				if (isStopped()) // exception is expected when the connection is first closed
					return;
				System.err.printf("Error in socket connection to Worker " + id + ": " + e);
				this.closeConnection();
			}
    	}
    }
}