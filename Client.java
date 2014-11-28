package mapreduce;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class Client {
	Socket socket; 

	public void sendFile(String filePath) throws IOException{
		socket = new Socket("127.0.0.1", 40001);
		OutputStream outStream = socket.getOutputStream();
		InputStream inStream = socket.getInputStream();

		//PrintWriter printWriter = new PrintWriter(outStream,true);
		outStream.write("MR_C\n".getBytes());
		outStream.flush();

		BufferedReader br = new BufferedReader(new InputStreamReader(inStream));
		System.out.println(br.readLine() + " received from Master"); //reads a "MR_C_OKAY" back from Master on the worker connection

		File myFile = new File(filePath);
		byte[] mybytearray = new byte[(int) myFile.length()];
		BufferedInputStream fileInputStream = new BufferedInputStream(new FileInputStream(myFile));
		fileInputStream.read(mybytearray, 0, mybytearray.length);
		fileInputStream.close();

		outStream.write(mybytearray);
		outStream.flush();

		socket.close();
	}

	public static void main(String[] args) throws IOException{
		new Client().sendFile("C:\\Users\\Kumar\\Desktop\\MR.java"); //send a file from Desktop to the Master
	}
}