package main;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;

public class Master {
    private ArrayList<Job> jobs = new ArrayList<>();
    private ServerSocket serverSocket;
    private Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;
    private int port;

    public Master(int port){
        this.port = port;
        startMaster();

        // Placeholder to test Jobs
        for(int i = 0; i < 10; i++){
            Random rand = new Random();
            char jobType = (char)(rand.nextInt(2) + 65);
            receive(new Job(Character.toString(jobType), i));
        }
        delegate();
    }

    private void startMaster(){
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server started");

            System.out.println("Waiting for a client...");

            socket = serverSocket.accept();
            System.out.println("Client accepted");
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }

    public void receive(Job job){
        jobs.add(job);
    }

    public void delegate(){
        try {
            out = new ObjectOutputStream(socket.getOutputStream());
            while (true) {
                for (Job job : jobs) {
                    try {
                        out.writeObject(job);
                    } catch (Exception io) {
                        io.printStackTrace();
                    }
                }
            }
        }
        catch (IOException io){
            io.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Master master = new Master(5000);
    }
}
