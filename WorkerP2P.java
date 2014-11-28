package mapreduce;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
 
public class WorkerP2P extends Thread {
 
    protected DatagramSocket socket = null;
    protected Job job;
    protected int port;
    protected ExecutorService exec;

    
    public WorkerP2P(int port, Job	job) throws IOException {
    	
    	this.exec = Executors.newCachedThreadPool();
    	this.port = port;
    	this.socket = new DatagramSocket(port);
    	this.job = job;
    	this.start();
    }
 
    public void run() {
 
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
    }
    
    public void closeConnection() {
    	socket.close();
    	exec.shutdown();
    }
    
    public void send(final String msg, final String address) {
    	// TODO the msg is larger than the receive buffer size
    	exec.execute(new Runnable() {
			public void run() {
				byte[] buf = msg.getBytes();
				DatagramPacket packet;
				try {
					packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(address), port);
	                socket.send(packet);
				} catch (IOException e) {
					System.err.println("Error in socket connection to other worker " + address + ":" + port);
				}
			}
		});
    }
}