package mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class ClientListener extends Thread {
	
	private static int connections = 0;
	
	protected ServerSocket clientSocket;
	protected Master master;
	protected InputStream in;
	protected OutputStream out;
	protected String MRFileName;
	protected boolean stopped;
	
	public ClientListener(Master m, int port) throws IOException {
		clientSocket = new ServerSocket(port);
		this.master = m;
		this.stopped = false;
	}
	
	protected synchronized boolean isStopped() {
		return stopped;
	}

	public synchronized void closeConnection() {
		stopped = true;
    	try {
    		in.close();
    		out.close();
			clientSocket.close();
    	} catch (IOException e) { }  //ignore exceptions since you are closing it anyways
    }

	public void run() {
		while(!isStopped()) {
			try {
				Socket client = this.clientSocket.accept();
				System.out.println("Client connected: " + client);
				in = client.getInputStream();
				out = client.getOutputStream();
				out.write(++connections);  //client waits for an ID
				MRFileName = Utils.receiveFile(in, master.basePath + File.separator);  // receive the MR java file
				out.write(Utils.ACK);  // notify client you received it
				List<String> filesToUse = Utils.readFilenames(in);  // receive files to operate on
				master.receiveMRJob(MRFileName, filesToUse, false);
			} catch (IOException e) {
				System.err.println("Error while accepting client connections: " + e);
			}
		}
	}
}