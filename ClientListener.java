package mapreduce;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class ClientListener extends Thread {
	
	protected ServerSocket clientSocket;
	protected Master master;
	protected InputStream in;
	protected OutputStream out;
	protected String MRFileName;
	protected static int connections = 0;
	
	public ClientListener(Master m) throws IOException {
		clientSocket = new ServerSocket(Utils.DEF_CLIENT_PORT);
		this.master = m;
	}
	
	private String receiveFileFromClient() {
		System.err.print("...Receiving MR job from Client node: ");
		try{
			// the first thing sent will be the filename
			int f;
			String name = "";
			while( (f = in.read()) != '\n') {
				name += (char) f;
			}
			int totalBytes = 0;
			byte[] mybytearray = new byte[1024];
			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(name));
			while (true){
				int bytesRead = in.read(mybytearray, 0, mybytearray.length);
				totalBytes += bytesRead;
				if (bytesRead <= 0) break;
				bos.write(mybytearray, 0, bytesRead);
				if (bytesRead < 1024) break;
			}
			System.err.printf("%d bytes read%n", totalBytes);
			bos.close();
			return name;
		} catch (IOException e) {
			System.err.println("Exception while receiving file from Client: " + e);
			return null;
		}
	}
	
	public void run() {
		while(true) {
			try {
				Socket client = this.clientSocket.accept();
				System.out.println("Client connected: " + client);
				in = client.getInputStream();
				out = client.getOutputStream();
				out.write(++connections);  //client waits for an ID

				boolean cont = true;
				while (cont) {
					int cmd;
					if ((cmd = in.read()) != 0) { //commands are one byte
						switch(cmd) {
						case Utils.C2M_UPLOAD:	
							MRFileName = receiveFileFromClient();
							break;
						case Utils.C2M_UPLOAD_FILES:
							List<String> filesToUse = master.getFilesList(in);
							master.setMRJob(MRFileName, filesToUse, true);
							cont = false;
							break;
						default:
							System.out.println("invalid command received at Client");
							cont = false;
							break;
						}
					}
				} 
			} catch (IOException e) {
				throw new RuntimeException("Error accepting client connections", e);
			}
		}
	}
}