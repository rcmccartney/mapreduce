package mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class' object is used by the worker to communicate with other worker peers.
 * This class' instance is always listening at the port specified for other W_P2P peers  
 *
 */
public class CopyOfWorkerP2P extends Thread {
 
	protected ServerSocket workerServerSocket;
	protected Worker worker;
    protected int port; //This is the port used at src and dest. Since all W_P2P communications assume this port
    protected ExecutorService exec; //Thread pool to use to send data out asynchronously
    protected boolean stopRun = false;
    
    public CopyOfWorkerP2P(int port, Worker worker) throws IOException {
    	this.exec = Executors.newCachedThreadPool();
    	this.port = port;   // == -1 ? Utils.DEF_WP2P_PORT: port;
    	workerServerSocket = new ServerSocket(this.port);
    	this.worker = worker;
    	this.setDaemon(true);
    	this.start();
    }
    
    public void receiveKV(Object K, Object V) {
		worker.currentJob.receiveKVAndAggregate(K, V);
    }
 
    private static class P2P extends Thread {
		
    	InputStream in;
    	OutputStream out;
    	CopyOfWorkerP2P container;
    	
		public P2P(CopyOfWorkerP2P container, Socket p2pSocket) throws IOException {
			this.container = container;
			in = p2pSocket.getInputStream();
			out = p2pSocket.getOutputStream();
		}
		
		public void run() {
			while (true) {
				try {
					if ( in.read() == Utils.W2W_KEY_TRANSFER) { //commands are one byte
						ObjectInputStream objInStream = new ObjectInputStream(in);
						Object[] objArr = (Object[]) objInStream.readObject();
						objInStream.close();
						out.write(Utils.W2W_KEY_TRANSFER_OKAY); 
						out.flush();
						System.out.println("hereKey " + objArr[0] + " hereVal " + objArr[1]);
						in.close();
						out.close();
						container.receiveKV(objArr[0], objArr[1]);
						break;
					}
				} catch (IOException | ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	
    public synchronized boolean isStopped() {
    	return stopRun;
    }
    
    /**
	 * Separate Thread: Listens for incoming messages
	 */
	public void run(){
		System.out.println("WorkerP2P listener info: " + workerServerSocket);
		while(!isStopped()) { //may be use !stopRun here
			try {
				Socket p2pSocket = workerServerSocket.accept();
				P2P connection = new P2P(this, p2pSocket);
				connection.setDaemon(true);  // this will cause exit upon user 'quit'
				connection.start();
			} catch (IOException e) {
				if(isStopped()) {
					System.err.println("P2P server stopped") ;
					return;
				}
				else 
					throw new RuntimeException("Error accepting worker P2P connection", e);
			}
		}
        System.out.println("P2P server stopped") ;
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
			System.out.println("make asocket");
			Socket socket = new Socket(peerAddress, port);
			System.out.println(socket);
			OutputStream out = socket.getOutputStream();
			InputStream in = socket.getInputStream();
			out.write(Utils.W2W_KEY_TRANSFER); 
			out.flush();
					
			ObjectOutputStream objOutStream = new ObjectOutputStream(out);
			objOutStream.writeObject(new Object[] {key, values});
			out.flush();
					
			if (in.read() != Utils.W2W_KEY_TRANSFER_OKAY)
				System.err.println("Invalid response received from Worker at " + peerAddress + "; port: " + port);
			
			out.close();
			in.close();
			socket.close();
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Close the Listener socket 
	 */
	public void closeConnection(){
		stopRun = true;
		try {
			workerServerSocket.close();
			exec.shutdown();
		} catch (IOException e) {
			System.out.println("IOException in RecNotifThread:stopRecNotif()");
			System.out.println(e);
		}
	}
	
	//For testing standalone
	public static void main(String[] args) throws IOException{
		new CopyOfWorkerP2P(-1, null);	
	}
}