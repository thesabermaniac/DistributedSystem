package main;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Slave implements Serializable {
    private final String slaveType;
    private transient Socket socket;
    private transient ObjectInputStream objectIn;
    private transient ObjectOutputStream objectOut;
//    private transient DataInputStream dataIn;
//    private transient DataOutputStream dataOut;
    public final List<Job> jobs = Collections.synchronizedList(new ArrayList<>());

    Slave(String slaveType) {
        this.slaveType = slaveType;
        connectToMaster();
        new JobReceiptThread().start();
        new DoJobsThread().start();
        new TimingRequestThread().start();
    }

    private void connectToMaster() {
        try {
            socket = new Socket("127.0.0.1", 5000);
            System.out.println("Connected");
            System.out.println(slaveTypeToString() + ": Connection to MASTER established.");

            objectOut = new ObjectOutputStream(socket.getOutputStream());
            Slave slave = this;
            objectOut.writeObject(slave);

            objectIn = new ObjectInputStream(socket.getInputStream());
//            objectOut.flush();

//            dataOut = new DataOutputStream(socket.getOutputStream());
//            dataIn = new DataInputStream(socket.getInputStream());

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    private void disconnectFromMaster() {
        try {
            objectIn.close();
            objectOut.close();
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
                while (true) {
                    Job newJob = (Job) objectIn.readObject();
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

            System.out.println("FINISHED: DoJobsThread");
        }
    }

    class TimingRequestThread extends Thread {

        public void run() {
            try {
//                dataOut = new DataOutputStream(socket.getOutputStream());
//                dataIn = new DataInputStream(socket.getInputStream());
                int time = 0;
                int signal = 0;
                while (true) {
                    signal = objectIn.readInt();

                    if (signal == -1) {
                        break;
                    }

                    time = computeTimeTillAllJobsFinished();
                    objectOut.write(time);
                    objectOut.flush();
                }

                System.out.println("FINISHED: TimingRequestThread");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private int computeTimeTillAllJobsFinished() {
        int total = 0;

        if (!jobs.isEmpty()) {
            for (Job job : jobs) {
                if (job.getJobType().equals(slaveType)) {
                    total += 2;
                } else {
                    total += 10;
                }
            }
        }

        System.out.println("Time until " + slaveTypeToString() + " is finished with its jobs: " + computeTimeTillAllJobsFinished());
        return total;
    }


    public static void main(String[] args) {
        Slave slave1 = new Slave("A");
        Slave slave2 = new Slave("B");
    }
}
