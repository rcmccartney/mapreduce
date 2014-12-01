package mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

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
        // tell the worker to send their files to you
        writeWorker(Utils.M2W_REQ_LIST);
    }
    
    public void writeWorker(final String arg, final byte... barg) {
    	try {
    		byte[] barr = Utils.concat(arg.getBytes(), barg);
    		out.write(barr);
    		out.flush();
    	} catch (IOException e) {
    		System.err.printf("Error writing to Worker %d: closing connection%n", id);
    		closeConnection();
    	}
    }

    public void writeObjToWorker(final Object obj) {
    	try {
    		ObjectOutputStream objStream = new ObjectOutputStream(out);
    		objStream.writeObject(obj);
    		objStream.flush();
    	} catch (IOException e) {
    		System.err.printf("Error writing to Worker %d: closing connection%n", id);
    		closeConnection();
    	}				
    }

    public void writeWorker(final String arg) {
    	try {
    		out.write(arg.getBytes());
    		out.flush();
    	} catch (IOException e) {
    		System.err.printf("Error writing to Worker %d: closing connection%n", id);
    		closeConnection();
    	}				
    }

    public void writeWorker(final byte... arg) {
    	try {
    		out.write(arg);
    		out.flush();
    	} catch (IOException e) {
    		System.err.printf("Error writing to Worker %d: closing connection%n", id);
    		closeConnection();
    	}				
    }
    
    public synchronized void closeConnection() {
    	stopped = true;
    	master.remove(id);
    	try {
    		if (!clientSocket.isClosed()) {
    			// don't use writeWorker since that will call close recursively
    			out.write(Utils.MR_QUIT); 
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
		case Utils.W2M_WP2P_PORT:
			master.workerIDAndPorts.put(this.id, readInt());
			break;
		case Utils.M2W_REQ_LIST_OKAY:
			List<String> wFiles = master.getFilesList(in);
			if (wFiles != null)
				master.addFiles(id, wFiles);
			break;
		case Utils.W2M_KEY:
			master.mj.receiveWorkerKey(in, this.id);
			break;
		case Utils.W2M_KEY_COMPLETE:
			//TODO: change wCount to compare with current actual # of workers on this job 
			master.mj.wCount++;
			if (master.mj.wCount == master.workerQueue.size()) // master now has all the keys from the workers
				master.mj.coordinateKeysOnWorkers();
			break;
		case Utils.W2M_KEYSHUFFLED:
			master.mj.wShuffleCount++;
			if(master.mj.wShuffleCount == master.workerQueue.size()) {
				System.err.println("...Shuffle & sort completed: starting reduction");
				master.writeAllWorkers(Utils.M2W_BEGIN_REDUCE);
			}
			break;
		case Utils.W2M_RESULTS:
			master.mj.receiveWorkerResults(in);
			break;
		case Utils.W2M_JOBDONE:
			master.mj.wDones++;
			if (master.mj.wDones == master.workerQueue.size()){
				master.mj.printResults();
			}
			break;
		default:
			System.err.println("Invalid command received on WorkerConnection: " + command);
			this.closeConnection();
			break;
		}
	} 
    
	public void sendFile(String name, byte[] bArr, byte transferType) {
		//notify client of pending MR transmission and send data
		writeWorker(transferType);
		writeWorker(name+'\n', bArr);  //newline critical
	}
    
	public byte[] readBytes() {
		try{		
			byte[] mybytearray = new byte[1024];
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			while (true) {
				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
				if (bytesRead <= 0) break;
				bos.write(mybytearray, 0, bytesRead);
				if (bytesRead < 1024) break;
			}
			bos.flush();
			return bos.toByteArray();
		} catch (IOException e) {
			System.err.println("Exception while receiving file from Client: " + e);
			return null;
		}
	}
	
	public int readInt() {
		try{		
			byte[] mybytearray = new byte[4];
			in.read(mybytearray, 0, mybytearray.length);
			return Utils.byteArrayToInt( mybytearray );
		} catch (IOException e) {
			System.err.println("Exception while receiving file from Client: " + e);
			return -1;
		}
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