package mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class WorkerConnection extends Thread {

    protected Socket clientSocket;
    protected int id;
	protected InputStream in;
	protected OutputStream out;
	protected boolean stopped = false;

    public WorkerConnection(Socket socket, int id) throws IOException {
        this.clientSocket = socket;
        out = clientSocket.getOutputStream();
        in = clientSocket.getInputStream();
        this.id   = id;
    }
    
    /**
     * 
     * @throws Exception
     */
    public synchronized void closeIOconnections() throws IOException {
    	stopped = true;
    	in.close();
    	out.close();
    }
    
    public synchronized boolean isStopped() {
    	return stopped;
    }

    public void run() {
    	
    	while(!isStopped()) {
			try {
				long time = System.currentTimeMillis();
				out.write(("HTTP/1.1 200 OK\n\nWorkerRunnable: " +
                    this.id + " - " +
                    time +
                    "").getBytes());
			} catch (IOException e) {
				if(isStopped()) {
					System.out.println("Worker connection stopped, cannot write to worker.") ;
					return;
				}
				else 
					throw new RuntimeException("Error accepting worker connection", e);
			}
    	}
    }
}