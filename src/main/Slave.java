package main;

import java.io.*;
import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class Slave implements Serializable {
    private final String slaveType;
    private transient Socket socket;
    private transient ObjectInputStream in;
    private transient ObjectOutputStream out;
    public final LinkedList<Job> jobs;

    Slave(String slaveType) {
        this.slaveType = slaveType;
        jobs = new LinkedList<>();
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

    private void disconnectFromMaster() {
        try {
            in.close();
            out.close();
            socket.close();
            System.out.println(slaveTypeToString() + ": Disconnected from MASTER.");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    private void sendMessageToMaster(String message) {
        try {
            System.out.println(message);
            out.writeObject(message);
            out.flush();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    private void receiveJobs() {
        JobReceiptThread jrt = new JobReceiptThread();
        jrt.start();
    }

    class JobReceiptThread extends Thread {

        JobReceiptThread() {
        }

        @Override
        public void run() {
            try {
                in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                while (true) {
                    Job newJob = (Job) in.readObject();
                    if (newJob.getJobType().equals("Completed")) {
                        sendMessageToMaster(slaveTypeToString() + " finished!");
                        break;
                    }

                    System.out.println("Type: " + newJob.getJobType() + "\tid: " + newJob.getId());
                    synchronized (jobs) {
                        jobs.add(newJob);
                    }
                }

                System.out.println("Finished");
            } catch (IOException | ClassNotFoundException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private void doJobs() {
        // TODO

        while (true) {
            if (jobs.isEmpty()) {
                continue;
            }

            Job currJob;
            synchronized (jobs) {
                currJob = jobs.get(0);
            }

            if (currJob.getJobType().equals("Completed")) {
                break;
            }

            try {
                if (currJob.getJobType().equals(this.slaveType)) {
                    Thread.sleep(2_000);
                } else {
                    Thread.sleep(10_000);
                }

                synchronized (jobs) {
                    jobs.remove(0);
                }

                // send a message back and say the job was finished
                sendMessageToMaster(slaveTypeToString() + " Job ID " + currJob.getId() + " has been completed.");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Finished");
    }

    public String getSlaveType() {
        return slaveType;
    }

    private String slaveTypeToString() {
        return "SLAVE-" + slaveType;
    }


    class DoJobsThread extends Thread {
        @Override
        public void run() {
            while (true) {
                synchronized (jobs) {
                    if (jobs.isEmpty()) {
                        continue;
                    }
                }

                Job currJob;
                synchronized (jobs) {
                    currJob = jobs.get(0);
                }

                if (currJob.getJobType().equals("Completed")) {
                    break;
                }

                try {
                    if (currJob.getJobType().equals(slaveType)) {
                        Thread.sleep(2_000);
                    } else {
                        Thread.sleep(10_000);
                    }

                    synchronized (jobs) {
                        jobs.remove(0);
                    }

                    // send a message back and say the job was finished
                    sendMessageToMaster(slaveTypeToString() + " Job ID " + currJob.getId() + " has been completed.");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("Finished");
        }
    }


    public int computeTimeTillAllJobsFinished() {
        AtomicInteger total = new AtomicInteger();

        if (jobs.isEmpty()) {
            sendMessageToMaster(slaveTypeToString() + ": This slave has no jobs to finish.");
            return total.get();
        }

        new Thread(() -> {
            synchronized (jobs) {
                for (Job job : jobs) {
                    if (job.getJobType().equals(slaveType)) {
                        total.addAndGet(2);
                    } else {
                        total.addAndGet(10);
                    }
                }
            }
        });

        return total.get();
    }


    public static void main(String[] args) {
        Slave slave = new Slave("A");
        Slave slave2 = new Slave("B");
//        slave.connectToMaster();
//        slave.sendMessageToMaster(slave.slaveTypeToString());
//        slave.receiveJob();
//        slave2.receiveJob();
//        slave.sendMessageToMaster("Finished!");
//        slave2.sendMessageToMaster("Finished!");
    }
}
