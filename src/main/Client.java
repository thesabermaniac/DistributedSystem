package main;
//todo user creates thread to send
//todo Thread which recieves

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.Scanner;

public class Client implements Serializable {
    private static transient Socket socket;
    private int port;
    static transient Scanner sc = new Scanner(System.in);
    static int idNumber = 0;
    static transient ObjectOutputStream out;
    static transient ObjectInputStream in;

    Client(int port) {
        this.port = port;
        connect();
    }

    private void connect() {
        try {
            socket = new Socket("127.0.0.1", port);
            System.out.println("Connected");

            out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(this);
            out.flush();
            // todo make to accept from user the job and number and create the class...
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void disconnectFromMaster() {
        try {
            in.close();
            out.close();
            socket.close();
            System.out.println("Client disconnected from MASTER, all jobs completed.");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public static String sendJobToMasterAndReceiveFromUser() throws InterruptedException, IOException {

        System.out.println("Please Select Either Job A or B: ");
        System.out.println("If you are done type Completed.");
        String jobType = sc.nextLine();
        while (!jobType.equals("A") && !jobType.equals("B") && !jobType.equals("Completed")) {
            System.out.println("Enter either A, B, or Completed.");
            jobType = sc.nextLine();
        }
        Job newJob = new Job(jobType, idNumber++);
        Thread thread = new SendToMaster(out, newJob);
        thread.start();
        thread.join();
        return jobType;
    }

    public static class UserAndMasterThread extends Thread {
        public void run() {

            try {
                while (!sendJobToMasterAndReceiveFromUser().equals("Completed")) {

                }
                System.out.println("Finished");
            } catch (InterruptedException | IOException e) {
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

        public void run() {
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


        public ReceiveFromMaster(ObjectInputStream inputStream) throws IOException {

            this.inputStream = inputStream;
//            this.job = job;
        }

        public void run() {
            Job j = new Job("", -1);
            while (!j.getJobType().equals("Completed")) {
                try {
                    j = (Job) inputStream.readObject();
                    if (!j.getJobType().equals("Completed")) {
                        System.out.println("Job " + j.getJobType() + " Completed.");

                    } else {
                        System.out.println("All Jobs Completed");
                    }
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        Client c = new Client(5000);
        UserAndMasterThread userAndMasterThread = new UserAndMasterThread();
//        ReceiveFromMaster receiveFromMaster = new ReceiveFromMaster(/*in*/); //todo check
        userAndMasterThread.start();
//        receiveFromMaster.start();
//        c.disconnectFromMaster();

        //todo disconnect send to master


    }
}
