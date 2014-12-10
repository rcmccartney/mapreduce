package mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client extends SocketClient {

	public Client(String[] args) {
		super(args);
	}
	
	public void sendJob(String jobFile, String...filePaths) {
		try {
			Path file = Paths.get(jobFile);
			byte[] filedata = Files.readAllBytes(file);
			Utils.write(out, Utils.C2M_UPLOAD, 
					file.getFileName().toString()+'\n', filedata);
			//otherwise data buffer reads into next message
			in.read();  // wait for reply from master before sending next message
			Utils.write(out, Utils.C2M_UPLOAD_FILES, filePaths);
			System.out.printf("%s uploaded to Master server from Client %d%n", jobFile, id);
			closeConnection();
		} catch (IOException e) {
			System.out.println("Error loading MR file: " + e);
		}
	}
		
	public static void main(String[] args) {
		//send a file from Desktop to the Master
		String[] clientArgs = { "-port", ""+Utils.DEF_CLIENT_PORT };
		new Client(clientArgs).sendJob("\\Jobs\\MRTest.java", "here", "there"); 
	}
}
