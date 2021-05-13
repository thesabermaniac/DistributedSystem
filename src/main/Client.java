package main;
//todo user creates thread to send
//todo Thread which recieves
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    private ServerSocket serverSocket;
    private static Socket socket;
    private static ObjectInputStream in;
    private static ObjectOutputStream out;
    private int port;
    static Scanner sc = new Scanner(System.in);
    static int idNumber = 0;

    Client(int port) {
        this.port = port;
        connect();
    }

    private void connect() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server started");

            System.out.println("Waiting for Master");

            socket = serverSocket.accept();
            System.out.println("Master connected");
            in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new ObjectOutputStream(socket.getOutputStream());
            // todo make to accept from user the job and number and create the class...
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private void disconnectFromMaster() {
        try {
            in.close();
            out.close();
            socket.close();
            System.out.println("Client disconnected from MASTER, all jobs completed.");
        } catch (IOException ioException) {ioException.printStackTrace();}
    }

    public static String sendJobToMasterAndReceiveFromUser() throws InterruptedException {
        System.out.println("Please Select Either Job A or B: ");
        System.out.println("If you are done type Completed.");
        String jobType = sc.nextLine();
        while (!jobType.equals("A") && !jobType.equals("B") && !jobType.equals("Completed")){
            System.out.println("Enter either A, B, or Completed.");
            jobType = sc.nextLine();
        }
        Job newJob = new Job(jobType, idNumber++);
        Thread thread = new SendToMaster(out, newJob);
        thread.start();
        thread.join();
        return jobType;
    }

    public static class UserAndMasterThread extends Thread{
        public void run(){
                try {
                    while (!sendJobToMasterAndReceiveFromUser().equals("Completed"));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    public static class SendToMaster extends Thread {
        final ObjectOutputStream outputStream;
        final Job job;

        public SendToMaster(ObjectOutputStream outputStream, Job job) {
            this.outputStream = outputStream;
            this.job = job;
        }

        public void run(){
            try {
                outputStream.writeObject(job);
                outputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReceiveFromMaster extends Thread {
        final ObjectInputStream inputStream;

        public ReceiveFromMaster(ObjectInputStream inputStream) {
            this.inputStream = inputStream;
//            this.job = job;
        }

        public void run(){
            String s = "";
            while (!s.equals("Completed")) {
                try {
                    s = (String) inputStream.readObject();
                    if (!s.equals("Completed")) {
                        System.out.println("Job " + s + " Completed.");
                    }
                    else{
                        System.out.println("All Jobs Completed");
                    }
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Client c = new Client(5000);
        UserAndMasterThread userAndMasterThread = new UserAndMasterThread();
        ReceiveFromMaster receiveFromMaster = new ReceiveFromMaster(in); //todo check
        userAndMasterThread.start();
        receiveFromMaster.start();
        c.disconnectFromMaster();
        //todo disconnect send to master


    }
}
