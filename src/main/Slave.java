package main;

import main.messages.JobRequest;
import main.messages.JobType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Slave implements Serializable {
    private final String slaveType;
    private Socket socket;
    private ObjectInputStream objectIn;
    private ObjectOutputStream objectOut;

    Slave(String slaveType) {
        this.slaveType = slaveType;
        connectToMaster();
        new ObjectReceiptThread().start();
//        new JobReceiptThread().start();
//        new DoJobsThread().start();
//        new TimingRequestThread().start();
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

    class ObjectReceiptThread extends Thread {

        @Override
        public void run() {
            try {
                while (true) {
                    Object receivedObject = objectIn.readObject();

                    if (receivedObject instanceof Job) {
                        if (((Job) receivedObject).getJobType().equals("Completed")) {
                            break;
                        }
//                        new DoJobsThread().start();
                        jobs.add((Job) receivedObject);
                        new DoJobsThread().start();
                    }
                    if (receivedObject instanceof Integer) {
                        new TimingRequestThread().start();
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
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

            synchronized (jobs) {
                Job currJob;
                currJob = jobs.get(0);

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
                Integer time = 0;
//                int signal = 0;
//                signal = objectIn.readInt();
//
//                if (signal == -1) {
//                    return;
//                }

                time = computeTimeTillAllJobsFinished();
                objectOut.writeObject(time);
                objectOut.flush();

                System.out.println("FINISHED: TimingRequestThread");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private int computeTimeTillAllJobsFinished() {
        int total = 0;

//        if (!jobs.isEmpty()) {

        for (Job job : jobs) {
            if (job.getJobType().equals(slaveType)) {
                total += 2;
            } else {
                total += 10;
            }
        }

//        }

//        System.out.println("Time until " + slaveTypeToString() + " is finished with its jobs: " + computeTimeTillAllJobsFinished());
        return total;
    }


    public static void main(String[] args) {
        Slave slave1 = new Slave("A");
        Slave slave2 = new Slave("B");
    }
}
