package main;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class Client implements Serializable {
    private final int port;
    private static final transient Scanner sc = new Scanner(System.in);
    private static int idNumber = 0;
    private static transient ObjectOutputStream out;
    private transient Socket socket;
    private boolean completed = false;

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
            new JobReceiptThread().start();
            sendJobToMasterAndReceiveFromUser();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void sendJobToMasterAndReceiveFromUser() throws InterruptedException, IOException {
        while (true) {
            String jobType = "";
            while (!jobType.equals("A") && !jobType.equals("B") && !jobType.equals("COMPLETED")) {
                System.out.println("Please Select Either Job A or B: ");
                System.out.println("Enter either A, B, or Completed.");
                jobType = sc.nextLine().toUpperCase();
            }
            Job newJob = new Job(jobType, idNumber++, this);
            Thread thread = new SendToMaster(out, newJob);
            thread.start();
            thread.join();
            if (jobType.equals("COMPLETED")) {
                completed = true;
                break;
            }
        }
    }

    private static class SendToMaster extends Thread {
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

    private class JobReceiptThread extends Thread{

        @Override
        public void run(){
            try {
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                while (true) {
                    Job newJob = (Job) in.readObject();
                    if (newJob.getJobType().equals("COMPLETED") && completed) {
                        System.out.println("All jobs have completed");
                        break;
                    }
                }

                System.out.println("Client - FINISHED: JobReceiptThread");
            }
            catch (IOException | ClassNotFoundException io){
                io.printStackTrace();
            }
        }
    }

    public static void main(String[] args){
        Client c = new Client(5000);
    }
}
