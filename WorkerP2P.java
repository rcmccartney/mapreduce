package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
public class WorkerP2P extends Thread {
 
    //protected DatagramSocket socket = null;
	protected ServerSocket workerServerSocket;
    //protected Job job;
	protected Worker worker;
    protected int port; //This is the port used at src and dest. Since all W_P2P communications assume this port
    protected ExecutorService exec; //Thread pool to use to send data out asynchronously
    protected boolean stopRun = false;
    
    public WorkerP2P(int port, Worker worker) throws IOException {
    	this.exec = Executors.newCachedThreadPool();
    	this.port = port;// == -1 ? Utils.DEF_WP2P_PORT: port;
    	workerServerSocket = new ServerSocket(this.port);
    	this.worker = worker;
    	this.start();
    }
 
    /**
	 * Separate Thread: Listens for incoming messages
	 */
	public void run(){
		System.out.println("WorkerP2P Listener info: " + workerServerSocket);
		try {
			while(!stopRun){ //may be use !stopRun here
				Socket p2pSocket = workerServerSocket.accept();
				InputStream in = p2pSocket.getInputStream();
				OutputStream out = p2pSocket.getOutputStream();
				//BufferedReader br = new BufferedReader(new InputStreamReader(in));

				byte cmd = (byte)in.read(); //commands are one byte
				switch(cmd){
					case Utils.W2W_KEY_TRANSFER:
						//Object[] objArr = Utils.gson.fromJson(br.readLine(), Object[].class);
						ObjectInputStream objInStream = new ObjectInputStream(in);
						Object[] objArr = (Object[])objInStream.readObject();
						objInStream = null;
						
						out.write(Utils.W2W_KEY_TRANSFER_OKAY); out.flush();
						
						worker.currentJob.receiveKVAndAggregate(objArr[0], objArr[1]);
						//K key = (K)objArr[0];
						//List<V> valList = (List<V>) objArr[1];
						//System.out.println("Recieved from Worker: ");
						//System.out.println("Key: " + objArr[0]);
						//System.out.println("List<Value>: " + objArr[1]);
						break;
					
					//TODO: may be Worker can send this message when done sending all the keys destined for this worker
					//case Utils.W2W_DONE_SENDING_KEYS:
						//break;
					
					default:
						System.out.println("invalid command received at WP2P");
						break;
				}
				//System.out.println("received: " + line);
				p2pSocket.close();
			}
		} catch (IOException | ClassNotFoundException e) {
			if(stopRun) return; //cuz if we intended to stop the server
			System.out.println("IOException in RecNotifThread:run()");
			System.out.println(e);
			//e.printStackTrace();
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
    	// TODO the msg is larger than the receive buffer size
    	//exec.execute(new Runnable() {
			//public void run() {
				//Used JSON instead of serializaton cuz its lightweight and easier
				//String jStr = Utils.gson.toJson(new Object[]{key, ls});
				//System.out.println("JSON String: " + jStr);
				try {
					Socket socket = new Socket(peerAddress, port);
					OutputStream out = socket.getOutputStream();
					InputStream in = socket.getInputStream();
					out.write(Utils.W2W_KEY_TRANSFER); out.flush();
					
					ObjectOutputStream objOutStream = new ObjectOutputStream(out);
					objOutStream.writeObject(new Object[]{key, values});
					out.flush();
					
					if (in.read() != Utils.W2W_KEY_TRANSFER_OKAY)
						System.err.println("Invalid response received from Worker at " + peerAddress + "; port: " + port);
					
					out.close();
					//out.write((jStr+"\n").getBytes());
					
					socket.close();
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			//}
		//});
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
			//e.printStackTrace();
		}
	}
	
	//For testing standalone
	public static void main(String[] args) throws IOException{
		new WorkerP2P(-1, null);	
	}

    /*public void run() {
 
        while (!job.worker.isStopped()) {
            try {
            	byte[] buf = new byte[512];
                // receive request
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                
            } catch (IOException e) {
            	if (job.worker.isStopped()) // exception is expected when the connection is first closed
					return;
				System.err.println("Error in socket connection to other workers: closing connection");
				this.closeConnection();     
            }
        }
    }*/
    
    /*public void send(final Object key, final List<Object> ls, final String peerAddress){
    	// TODO the msg is larger than the receive buffer size
    	exec.execute(new Runnable() {
			public void run() {
				///////////////////////////////
				
				String jStr = other.Utils.gson.toJson(key);
				ByteArrayInputStream byteArrIS  = new ByteArrayInputStream(jStr.getBytes());
				byte[] bytePacketArr = new byte[576];
				DatagramPacket packet;
				while(true){
					int bytesRead = byteArrIS.read(bytePacketArr, 0, bytePacketArr.length);
					if (bytesRead <= 0) break;
					// prob try bytePacketArr.length as the 2nd param
	                try {
	                	packet = new DatagramPacket(bytePacketArr, bytesRead, 
	                			InetAddress.getByName(peerAddress), port);
						socket.send(packet);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	                
					if (bytesRead < bytePacketArr.length) break;
				}
				
				///////////////////////////////
			}
		});
    }*/
}