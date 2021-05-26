package main;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class Slave implements Serializable {
    private final String slaveType;
    private transient Socket socket;
    private transient ObjectOutputStream out;
    private final List<Job> jobs = Collections.synchronizedList(new ArrayList<>());

    Slave(String slaveType) {
        this.slaveType = slaveType;
        connectToMaster();
        new JobReceiptThread().start();
        new DoJobsThread().start();
    }

    private void connectToMaster() {
        try {
            socket = new Socket("127.0.0.1", 5000);
            System.out.println("Connected");
            System.out.println(slaveTypeToString() + ": Connection to MASTER established.");

            out = new ObjectOutputStream(socket.getOutputStream());
            Slave slave = this;
            out.writeObject(slave);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    class JobReceiptThread extends Thread {

        JobReceiptThread() {
        }

        @Override
        public void run() {
            try {
                ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                while (true) {
                    Job newJob = (Job) in.readObject();
                    if (newJob.getJobType().equals("COMPLETED")) {
                        jobs.add(newJob);
                        break;
                    }

                    System.out.println(slaveTypeToString() + " Received " + newJob.toString());
                    jobs.add(newJob);
                }

                System.out.println(slaveTypeToString() + " - FINISHED: JobReceiptThread");
            } catch (IOException | ClassNotFoundException ioException) {
                ioException.printStackTrace();
            }
        }
    }


    public String getSlaveType() {
        return slaveType;
    }

    public String slaveTypeToString() {
        return "SLAVE-" + slaveType;
    }


    class DoJobsThread extends Thread {
        private final AtomicBoolean running = new AtomicBoolean(false);

        public void end(){
            running.set(false);
        }

        @Override
        public void run() {
            running.set(true);
            while (running.get()) {
                if (jobs.isEmpty()) {
                    continue;
                }

                Job currJob;
                currJob = jobs.get(0);

                if (currJob.getJobType().equals("COMPLETED")) {
                    this.end();
                    break;
                }

                try {
                    if (currJob.getJobType().equals(slaveType)) {
                        Thread.sleep(2_000);
                    } else {
                        Thread.sleep(10_000);
                    }
                    out.writeObject(currJob);

                    jobs.remove(0);

                    System.out.println(slaveTypeToString() + " completed " + currJob.toString());
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }

            System.out.println(slaveTypeToString() + " - FINISHED: DoJobsThread");
        }
    }



    public static void main(String[] args) {
        Slave slave = new Slave("A");
        Slave slave2 = new Slave("B");
    }
}
