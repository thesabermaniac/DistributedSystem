package main;

import java.io.BufferedInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Slave {
    private final String slaveType;
    private Socket socket;
    private ServerSocket serverSocket;
    private ObjectInputStream in;
    private DataOutputStream out;
    // private ArrayList<Job> jobs;

    Slave(String slaveType) {
        this.slaveType = slaveType;
        connectToMaster();
    }

    private void connectToMaster() {
        try {
            serverSocket = new ServerSocket(5000);
            System.out.println("SLAVE " + slaveType + ": Awaiting connection to MASTER...");

            socket = serverSocket.accept();
            System.out.println("SLAVE " + slaveType + ": Connection to MASTER established.");

            in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
            out = new DataOutputStream(socket.getOutputStream());
        } catch (IOException unknownHostException) {
            unknownHostException.printStackTrace();
        }
    }

    private void disconnectFromMaster() {
        try {
            in.close();
            out.close();
            socket.close();
            System.out.println("SLAVE " + slaveType + ": Disconnected from MASTER.");
        } catch (IOException ioException) {
            System.out.println(ioException.toString());
        }
    }

    private void acceptJob() {
        // TODO
    }

    private void doJob() {
        // TODO
        // get next job in queue
        // if it's this.slaveType, sleep thread for 2 seconds
        // if it's not the type, sleep thread for 10 seconds
        // send a message back and say the job was finished
    }


    public String getSlaveType() {
        return slaveType;
    }
}
