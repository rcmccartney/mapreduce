package mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client extends Worker {

	public Client(String[] args) {
		super(args);
	}

	protected byte[] filedata;
	
	public void sendFile(String filePath) {
		try {
			Path file = Paths.get(filePath);
			filedata = Files.readAllBytes(file);
			super.writeMaster(mapreduce.Utils.MR_C);
		} catch (IOException e) {
			System.out.println("Error loading MR file");
			this.closeConnection();
		}
	}
		
	@Override
    public void receive(int command) {
    	if (command == Utils.MR_C_OKAY) {
    		super.writeMaster(filedata);
			System.out.println("Java file uploaded to Master server.");
    		this.closeConnection();
		}
    }

	public static void main(String[] args) {
		new Client(args).sendFile("C:\\Users\\mccar_000\\Desktop\\MR.java"); //send a file from Desktop to the Master
	}
}