package main;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Slave {
    private final String slaveType;
    private Socket socket;
    private ObjectInputStream in;
    private DataOutputStream out;
    private final LinkedList<Job> jobs;

    Slave(String slaveType) {
        this.slaveType = slaveType;
        jobs = new LinkedList<>();
        connectToMaster();
    }

    private void connectToMaster() {
        try {
            ServerSocket serverSocket = new ServerSocket(5000);
            System.out.println(slaveTypeToString() + ": Awaiting connection to MASTER...");

            socket = serverSocket.accept();
            System.out.println(slaveTypeToString() + ": Connection to MASTER established.");

            in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(socket.getOutputStream());
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
            out.writeUTF(message);
            out.flush();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    private void receiveJob() {
        try {
            Job newJob = (Job) in.readObject();
            synchronized (jobs) {
                jobs.add(newJob);
            }
        } catch (IOException | ClassNotFoundException ioException) {
            ioException.printStackTrace();
        }
    }

    private void doJob() {
        // TODO

        // make sure the queue's not empty
        if (jobs.isEmpty()) {
            sendMessageToMaster(slaveTypeToString() + ": This slave has no jobs to finish.");
            return;
        }

        // get next job in queue
        AtomicReference<Job> curJob = new AtomicReference<>();

        new Thread(() -> {
            synchronized (jobs) {
                curJob.set(jobs.peekFirst());
            }

            try {
                if (curJob.get() != null) {
                    if (curJob.get().getJobType().equals(slaveType)) {
                        // this is a job of the same type and it is therefore "optimized"
                        Thread.sleep(2000);
                    } else {
                        // this is a job of a different type and it is therefore "unoptimized"
                        Thread.sleep(10_000);
                    }
                }

                // make sure to remove the finished job
                synchronized (jobs) {
                    jobs.removeFirst();
                }
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
        });

        // send a message back and say the job was finished
        sendMessageToMaster(slaveTypeToString() + "Job ID " + curJob + " has been completed.");
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

    public String getSlaveType() {
        return slaveType;
    }

    private String slaveTypeToString() {
        return "SLAVE-" + slaveType;
    }
}
