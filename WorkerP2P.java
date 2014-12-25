package mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

/**
 * This class' object is used by the worker to communicate with other worker peers.
 * This class' instance is always listening at the port specified for other W_P2P peers  
 *
 */
public class WorkerP2P extends Thread {
 
	protected ServerSocket workerServerSocket;
	protected Worker worker;
    protected boolean stopped;
    
    public WorkerP2P(int port, Worker worker) throws IOException {
    	this.stopped = false;
    	this.workerServerSocket = new ServerSocket(port);
    	this.worker = worker;
    	this.setDaemon(true);
    	this.start();
    }
    
    protected synchronized boolean isStopped() {
    	return stopped;
    }
 
    /**
	 * Separate Thread: Listens for incoming messages
	 */
	public void run(){
		System.out.println("WorkerP2P Listener info: " + workerServerSocket);
		try {
			while(!isStopped()) {
				Socket p2pSocket = workerServerSocket.accept();
				int jobID = Utils.readInt(p2pSocket.getInputStream());
				Object[] objArr = (Object[]) 
						new ObjectInputStream(p2pSocket.getInputStream()).readObject();
				System.out.println("Job " + jobID + ": Received <" + objArr[0] + "> from " + p2pSocket);
				worker.jobs.get(jobID).receiveKV(objArr[0], objArr[1]);
			}
		} catch (IOException | ClassNotFoundException e) {
			if (isStopped()) // we intended to stop the server
				return;
			System.out.println("IOException in WorkerP2P: " + e);
		}
	}
    
	/**
	 * Adhoc socket communication
	 * Send command, wait for command_okay, and then send Key,Value pair in an Object[] as JSON 
	 * @param key
	 * @param ls
	 * @param peerAddress
	 * @param port
	 */
	public <K,V> void send(K key, List<V> values, int jobID, String peerAddress, int port){
		try {
			Socket socket = new Socket(peerAddress, port);
			Utils.writeInt(socket.getOutputStream(), jobID);
			new ObjectOutputStream(socket.getOutputStream()).writeObject(new Object[]{key, values});
			socket.close();
			System.out.println("Job " + jobID + ": Sent <" + key + "> to " + peerAddress + ":" + port);
		} catch (IOException e) {
			System.err.println("Error sending K,V pair between workers: " + e);
		} 
	}
	
	/**
	 * Close the Listener socket 
	 */
	public synchronized void closeConnection(){
		stopped = true;
		try {
			workerServerSocket.close();
		} catch (IOException e) {}  //ignore exceptions since you are closing
	}	
}