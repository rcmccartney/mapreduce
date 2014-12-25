package mapreduce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Client extends SocketClient {

	public Client(String[] args) {
		super(args);
	}
	
    @Override
    public void usageTemplate() {
		System.out.println("Correct usage: java Client <jobFile> [<file1> <file2>...] [-h <hostName>] [-p <port>]");
		System.out.println("\t<jobFile>: A source file that extends Mapper.");	
		System.out.println("\t<file1> <file2>...: Files to be operated on. Omit to use all Worker files.");	
		System.out.println("\t-h: override localhost to set the host to <hostName>.");
		System.out.println("\t-p: override default port 40000 to <port>.");
		System.out.println("\t<host> and <port> must match the Master Server's Client connection.");
		System.exit(1);
    }
	
	public void sendJob(String jobFile, String...filePaths) {
		try {
			Path file = Paths.get(jobFile);
			byte[] filedata = Files.readAllBytes(file);
			Utils.writeFile(out, file.getFileName().toString(), filedata);
			in.read();  // wait for reply from master before sending next message
			Utils.writeFilenames(out, filePaths);
			System.out.printf("%s uploaded to Master server from Client %d%n", jobFile, id);
			closeConnection();
		} catch (IOException e) {
			System.out.println("Error loading MR file: " + e);
		}
	}
		
	public static void main(String[] args) {
		//send a file from Desktop to the Master
		String job = args[0];
		ArrayList<String> jobFiles = new ArrayList<>();
		ArrayList<String> clientArgs = new ArrayList<>();
		
		//override the default port that the worker uses for a client
		clientArgs.add("-p");
		clientArgs.add(""+Utils.DEF_CLIENT_PORT);  // this is position 1

		for(int i = 1; i < args.length; i++) {
			if (args[i].equals("-h")) {
				clientArgs.add("-h");
				clientArgs.add(args[++i]);
			}
			else if (args[i].equals("-p")) {
				clientArgs.remove(1); 
				clientArgs.add(1, args[++i]);				
			}
			else 
				jobFiles.add(args[i]);
		}
		new Client(clientArgs.toArray(new String[clientArgs.size()]))
				.sendJob(job, jobFiles.toArray(new String[jobFiles.size()])); 
	}
}
