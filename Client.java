package mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client extends Worker {

	public Client(String[] args) {
		super(args);
	}
	
	public void sendJob(String jobFilePath, String...filePaths) {
		try {
			Path file = Paths.get(jobFilePath);
			byte[] filedata = Files.readAllBytes(file);
			writeMaster(Utils.C2M_UPLOAD);
    		writeMaster(file.getFileName().toString()+'\n', filedata);
			// otherwise data buffer reads into next message
    		try { Thread.sleep(10); } catch (Exception e) {}
    		writeMaster(Utils.C2M_UPLOAD_FILES);
    		writeMaster(filePaths.length);
    		for (String fileToUse : filePaths) {
    			writeMaster(fileToUse);
    		}
			System.out.println("Java file uploaded to Master server.");
    		closeConnection();
		} catch (IOException e) {
			System.out.println("Error loading MR file");
			closeConnection();
		}
	}
		
	public static void main(String[] args) {
		//send a file from Desktop to the Master
		new Client(args).sendJob("C:\\Users\\mccar_000\\Desktop\\MRTest.java", "here", "there"); 
	}
}