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
    protected boolean stopRun;
    
    public WorkerP2P(int port, Worker worker) throws IOException {
    	this.stopRun = false;
    	this.workerServerSocket = new ServerSocket(port);
    	this.worker = worker;
    	this.setDaemon(true);
    	this.start();
    }
 
    /**
	 * Separate Thread: Listens for incoming messages
	 */
	public void run(){
		System.out.println("WorkerP2P Listener info: " + workerServerSocket);
		try {
			while(!stopRun) {
				Socket p2pSocket = workerServerSocket.accept();
				Object[] objArr = (Object[]) 
						new ObjectInputStream(p2pSocket.getInputStream()).readObject();
				worker.currentJob.receiveKV(objArr[0], objArr[1]);
				System.out.println("Received <key,List<V>> from " + p2pSocket);
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
	public <K,V> void send(final K key, final List<V> values, final String peerAddress, final int port){
		try {
			Socket socket = new Socket(peerAddress, port);
			new ObjectOutputStream(socket.getOutputStream()).writeObject(new Object[]{key, values});
			socket.close();
			System.out.println("Sent <" + key + ",List<V>> to " + peerAddress + ":" + port);
		} catch (IOException e) {
			System.err.println("Error sending K,V pair between workers: " + e);
		} 
	}
	
	// access the boolean in a synchronized manner
	protected synchronized boolean isStopped() {
		return stopRun;
	}
	
	/**
	 * Close the Listener socket 
	 */
	public synchronized void closeConnection(){
		stopRun = true;
		try {
			workerServerSocket.close();
		} catch (IOException e) {
			System.out.println("IOException in closing WorkerP2P connection: " + e);
		}
	}
	
	// each worker is on a unique port, can use this for equality testing
	// but should change this to be more scalable
	public boolean equals(String peerAddress, Integer peerPort) {
		return workerServerSocket.getLocalPort() == peerPort;
	}
}