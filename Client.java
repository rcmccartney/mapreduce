package mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client extends Worker {

	protected byte[] filedata;
	protected Path file;
	
	public Client(String[] args) {
		super(args);
	}
	
	public void sendFile(String filePath) {
		try {
			file = Paths.get(filePath);
			filedata = Files.readAllBytes(file);
			writeMaster(mapreduce.Utils.C2M_UPLOAD);
		} catch (IOException e) {
			System.out.println("Error loading MR file");
			closeConnection();
		}
	}
		
	@Override
    public void receive(int command) {
    	if (command == Utils.C2M_UPLOAD_OKAY) {
    		// newline is critical to separate filename from contents
    		writeMaster(file.getFileName().toString()+'\n', filedata);
			System.out.println("Java file uploaded to Master server.");
    		closeConnection();
		}
    }

	public static void main(String[] args) {
		new Client(args).sendFile("C:\\Users\\mccar_000\\Desktop\\MR.java"); //send a file from Desktop to the Master
	}
}