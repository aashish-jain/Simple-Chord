package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Server extends Thread {
    static final int SERVER_PORT = 10000;
    static final String TAG = "SERVER_TASK";


    public void run() {
        /* Open a socket at SERVER_PORT */
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        /* Accept a client connection and spawn a thread to respond */
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                new ServerThread(socket).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

/* https://stackoverflow.com/questions/10131377/socket-programming-multiple-client-to-one-server*/
class ServerThread extends Thread {
    ObjectOutputStream oos;
    ObjectInputStream ois;

    static final String TAG = "SERVER_THREAD";


    public  ServerThread(Socket socket) {
        try {
            this.ois = new ObjectInputStream(socket.getInputStream());
            this.oos = new ObjectOutputStream(socket.getOutputStream());
            Log.d(TAG, "Server started");
        } catch (Exception e) {
            Log.e(TAG, "Failure");
        }
    }

    @Override
    public void run() {
        //Read from the socket
        try {
            while (true) {
                Thread.sleep(100000000);
                //TODO: Make server respond to queries
            }
        } catch (Exception e) {
            Log.e(TAG, "Failure");
        }
    }
}