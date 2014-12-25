package mapreduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class CommandLine extends Thread {

	private Master m;
	
	public CommandLine(Master m) {
		this.m = m;
	}
	
	/**
	 * This is how a user interacts with the Master node of the system.  
	 * It is run by the main thread of execution
	 * @throws UnknownHostException 
	 * 
	 */
	public void run() {
		System.out.println("#################################################");
		System.out.println("#\t\tMAP-REDUCE FRAMEWORK\t\t#");
		System.out.println("#\t\t\t\t\t\t#");
		try {
			System.out.println("# Server:"+InetAddress.getLocalHost().getHostAddress()+"\t\t\t\t#");
		} catch (UnknownHostException e) {
			System.out.println("#\t\t\t\t\t\t#");
		}
		System.out.println("# Port:"+m.port+"\t\t\t\t\t#");	
		System.out.println("# Type help or man to view the man pages\t#");
		System.out.println("#\t\t\t\t\t\t#");
		System.out.println("#################################################");
		Scanner input = new Scanner(System.in);
		do {
			System.out.print("> ");
			String command;
			command = input.nextLine().trim();
			String[] line = command.split("\\s+");
			if (line[0].equalsIgnoreCase("man") || 
					(line[0].equalsIgnoreCase("help") && line.length==1))
				printFull();
			else if (line[0].equalsIgnoreCase("help"))
				if (line[1].equalsIgnoreCase("ls"))
					printLS();
				else if (line[1].equalsIgnoreCase("man"))
					printMan();
				else if (line[1].equalsIgnoreCase("q"))
					printQ();
				else if (line[1].equalsIgnoreCase("help"))
					printHelp();
				else if (line[1].equalsIgnoreCase("ld"))
					printLD();
				else if (line[1].equalsIgnoreCase("lf"))
					printLF();
				else
					unrecognized(line[1]);
			else if (line[0].equalsIgnoreCase("ls")) {
		        // tell the workers to send their files to you
		        m.writeAllWorkers(Utils.M2W_REQ_LIST);
				synchronized (m.queueLock) {
					for (WorkerConnection wc : m.workerQueue) {
						System.out.println(wc);
						if (line.length > 1 && line[1].equals("-l"))
							printFiles(wc.id);
					}
				}
			}
			else if (line[0].equalsIgnoreCase("q")) {
				System.out.printf("Really quit? There %s %d job%s pending%n> ", 
						(m.getJobs()==1?"is":"are"), m.getJobs(), (m.getJobs()==1?"":"s"));
				command = input.nextLine().trim();
				if (command.equalsIgnoreCase("y") || command.equalsIgnoreCase("yes")) 
					m.stopServer();
			}
			else if (line[0].equalsIgnoreCase("ld")) {
				if (line.length > 1) {
					List<String> filesToUse = new LinkedList<>();
					for(int i = 2; i < line.length; i++)
						filesToUse.add(line[i]);
					try {
						m.receiveMRJob(line[1], filesToUse, false);
					} catch (Exception e) {
						input.close();
						throw new RuntimeException(e);
					}
				}
				else {
					System.out.printf("Enter filename (operates on all worker files):%n> ");
					command = input.nextLine().trim();
					try {
						m.receiveMRJob(command, new LinkedList<String>(), false);
					} catch (Exception e) {
						input.close();
						throw new RuntimeException(e);
					}
				}
			}
			else if (line[0].equalsIgnoreCase("lf")) {
				if (line.length >= 3) {
					for( int i = 2; i < line.length; i++)
						m.sendRegularFile(line[1], line[i]);
				}
				else {
					System.out.printf("Enter filename:%n> ");
					command = input.nextLine().trim();
					System.out.printf("Enter worker IDs:%n> ");
					String[] wkrs = input.nextLine().split("\\s+");
					for( String wkr : wkrs)
						m.sendRegularFile(command, wkr);
				}
			}
			else 
				unrecognized(line[0]);
		} while (!m.isStopped());
		input.close();
	}
	
    private void printFiles(int workerID) {
    	if (m.IDtoFiles.containsKey(workerID)) {
    		List<String> l = m.IDtoFiles.get(workerID);
    		for (String file : l) 
    			System.out.println("  " + file);
    	}
    }
	
	protected void unrecognized(String cmd) {
		if (cmd.length() > 0)
			System.out.println(cmd + " is not recognized as a valid input command");
	}
	
	protected void printFull() {
		printMan();
		printHelp();
		printLS();
		printLD();
		printLF();
		printQ();
	}
	
	protected void printHelp() {
		System.out.println("help <cmd>: get further information on <cmd>");
	}
	
	protected void printQ() {
		System.out.println("q: quit the system (y to confirm)");
	}
	
	protected void printMan() {
		System.out.println("man: display manual");
	}
	
	protected void printLS() {
		System.out.println("ls [-l]: list the workers currently in the cluster [with files]");
	}
	
	protected void printLD() {
		System.out.println("ld [filename]: load the map-reduce job");
	}
	
	protected void printLF() {
		System.out.println("lf [filename] <workerID1 [workerID2]...>: load the file to workerID");
	}
}
