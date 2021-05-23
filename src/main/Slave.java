package main;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Slave implements Serializable {
    private final String slaveType;
    private transient Socket socket;
    private transient ObjectInputStream in;
    private transient ObjectOutputStream out;
    public final List<Job> jobs = Collections.synchronizedList(new ArrayList<>());

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
                        break;
                    }

                    System.out.println("Type: " + newJob.getJobType() + "\tid: " + newJob.getId());
                    jobs.add(newJob);
                }

                System.out.println("Finished");
            } catch (IOException | ClassNotFoundException ioException) {
                ioException.printStackTrace();
            }
        }
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
                if (jobs.isEmpty()) {
                    continue;
                }

                Job currJob;
                currJob = jobs.get(0);

                if (currJob.getJobType().equals("Completed")) {
                    break;
                }

                try {
                    if (currJob.getJobType().equals(slaveType)) {
                        Thread.sleep(2_000);
                    } else {
                        Thread.sleep(10_000);
                    }

                    jobs.remove(0);

                    System.out.println(slaveTypeToString() + " Job ID " + currJob.getId() + " has been completed");
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
            return total.get();
        }

        new Thread(() -> {
            for (Job job : jobs) {
                if (job.getJobType().equals(slaveType)) {
                    total.addAndGet(2);
                } else {
                    total.addAndGet(10);
                }
            }
        });

        return total.get();
    }


    public static void main(String[] args) {
        Slave slave = new Slave("A");
        Slave slave2 = new Slave("B");
    }
}
