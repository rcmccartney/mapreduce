package mapreduce;

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
	
	public ClientListener(Master m) throws IOException {
		clientSocket = new ServerSocket(Utils.DEF_CLIENT_PORT);
		this.master = m;
		this.stopped = false;
	}
	
	protected synchronized boolean isStopped() {
		return stopped;
	}

	public synchronized void closeConnection() {
		stopped = true;
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

	public void run() {
		while(!isStopped()) {
			try {
				Socket client = this.clientSocket.accept();
				System.out.println("Client connected: " + client);
				in = client.getInputStream();
				out = client.getOutputStream();
				out.write(++connections);  //client waits for an ID
				// boolean for this single client connection
				boolean currConnection = true;
				while (currConnection) {
					int cmd;
					if ((cmd = in.read()) != -1) { //commands are one byte
						switch(cmd) {
						case Utils.C2M_UPLOAD:	
							// receive it in current directory
							MRFileName = Utils.receiveFile(in, "");
							out.write(Utils.AWK);
							// don't quit current connection, Client can send more files
							break;
						case Utils.C2M_UPLOAD_FILES:
							List<String> filesToUse = Utils.getFilesList(in);
							// false bc it is an external connection
							master.setMRJob(MRFileName, filesToUse, false);
							currConnection = false;
							break;
						default:
							System.out.println("Invalid command received at Client: " + cmd);
							currConnection = false;
							break;
						}
					}
				} 
			} catch (IOException e) {
				throw new RuntimeException("Error accepting client connections: ", e);
			}
		}
	}
}