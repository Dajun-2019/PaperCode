package org.Demo;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

public class SimpleHTTPServer {
    public static void main(String[] args) throws IOException {
        //If you connect to http://localhost:8080 from your browser,
        //the connection will be established and the browser will wait forever.
        ServerSocket server = new ServerSocket(8080);
        System.out.println("Listening for connection on port 8080 ....");
        while (true){
            //accept incoming connection by blocking call to accept() method
            final Socket client = server.accept();

            // 1. Read HTTP request from the client socket
//            InputStreamReader inputStreamReader = new InputStreamReader(client.getInputStream());
//            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
//            String line = bufferedReader.readLine();
//            while (!line.isEmpty()){
//                System.out.println(line);
//                line = bufferedReader.readLine();
//            }
            // 2. Prepare an HTTP response
            Date today = new Date();
            String httpResponse = "HTTP/1.1 200 OK\r\n\r\n" + today;
            // 3. Send HTTP response to the client
            try (Socket socket = server.accept()){
                socket.getOutputStream().write(httpResponse.getBytes("UTF-8"));
            }
            // 4. Close the socket
            client.close();
        }
    }
}
