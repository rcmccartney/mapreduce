package mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client extends Worker {

	public Client(String[] args) {
		super(args);
	}
	
	public void sendFile(String filePath) {
		try {
			Path file = Paths.get(filePath);
			byte[] filedata = Files.readAllBytes(file);
			writeMaster(Utils.C2M_UPLOAD);
    		writeMaster(file.getFileName().toString()+'\n', filedata);
			System.out.println("Java file uploaded to Master server.");
    		closeConnection();
		} catch (IOException e) {
			System.out.println("Error loading MR file");
			closeConnection();
		}
	}
		
	public static void main(String[] args) {
		new Client(args).sendFile("C:\\Users\\mccar_000\\Desktop\\MRTest2.java"); //send a file from Desktop to the Master
	}
}