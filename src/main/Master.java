package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class Master {
    public static final CopyOnWriteArrayList<Job> jobs = new CopyOnWriteArrayList<>();
    private static final CopyOnWriteArrayList<Job> slaveAJobs = new CopyOnWriteArrayList<>();
    private static final CopyOnWriteArrayList<Job> slaveBJobs = new CopyOnWriteArrayList<>();
    private final HashMap<Socket, Object> activeClients = new HashMap<>();
    private static Socket socket;
    private final int port;
    private static ObjectInputStream in;

    public Master(int port){
        this.port = port;
        startMaster();
    }

    private void startMaster() {
        ServerSocket serverSocket = null;
        System.out.println("Server started");

        System.out.println("Waiting for a slave...");
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            while (activeClients.keySet().size() < 4) {
                socket = serverSocket.accept();

                System.out.println("New client connected");
                in = new ObjectInputStream(socket.getInputStream());
                Object input = in.readObject();
                if (input instanceof Slave) {
                    Slave slave = (Slave) input;
                    activeClients.put(socket, slave);
                    System.out.println("Socket: " + socket + ", slave: " + slave);
                } else {
                    Client client = (Client) input;
                    activeClients.put(socket, client);
                    System.out.println("Socket: " + socket + ", client: " + client);
                }

                delegate();
                receiveMessage();
            }
            System.out.println("Closed connections");
        } catch (Exception io) {
            io.printStackTrace();
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void delegate() throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

        Thread t = new OutputThread(socket, activeClients, out);

        t.start();

    }

    public void receiveMessage() {
        try {
            InputThread thread = new InputThread(socket, in);
            thread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int computeRuntime(CopyOnWriteArrayList<Job> slaveJobs, String slaveType){
        int total = 0;
        for(Job j:slaveJobs){
            if(j.getJobType().equals(slaveType)){
                total += 2;
            }
            else {
                total += 10;
            }
        }
        return total;
    }

    static class OutputThread extends Thread {
        final Socket socket;
        HashMap<Socket, Object> activeClients;
        ObjectOutputStream output;
        private final AtomicBoolean running = new AtomicBoolean(false);

        public OutputThread(Socket s, HashMap<Socket, Object> clients, ObjectOutputStream out) {
            activeClients = clients;
            socket = s;
            output = out;
        }

        public void end(){running.set(false);}

        private String getTargetSlave(Job job){

            int slaveATotal = computeRuntime(slaveAJobs, "A");
            int slaveBTotal = computeRuntime(slaveBJobs, "B");
            String targetSlave;
            if(job.getJobType().equals("A")){
                slaveATotal += 2;
                slaveBTotal += 10;
            }
            else {
                slaveATotal += 10;
                slaveBTotal += 2;
            }
            if(slaveATotal < slaveBTotal){
                targetSlave = "A";
            }
            else {
                targetSlave = "B";
            }

            return targetSlave;
        }

        @Override
        public void run() {
            try {
                Job job;
                running.set(true);
                while (running.get()) {
                    synchronized (jobs) {
                        if (jobs.size() > 0) {
                            if (activeClients.get(socket) instanceof Slave) {
                                job = jobs.get(0);
                                Slave slave = (Slave) activeClients.get(socket);

                                if (job.getJobType().equals("COMPLETED")) {
                                    output.writeObject(job);
                                    output.flush();
                                    this.end();
                                    break;
                                }

                                if (slave.getSlaveType().equals(getTargetSlave(job))) {
                                    output.writeObject(job);
                                    output.flush();
                                    System.out.println(job.toString() + " sent to " + slave.slaveTypeToString());
                                    if(slave.getSlaveType().equals("A")){
                                        slaveAJobs.add(job);
                                    }
                                    else {
                                        slaveBJobs.add(job);
                                    }
                                    jobs.remove(job);
                                }
                            }
                            else {
                                this.end();
                            }
                        }
                    }
                }

                System.out.println("FINISHED: OutputThread");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class InputThread extends Thread {
        Socket socket;
        ObjectInputStream input;

        public InputThread(Socket socket, ObjectInputStream in) {
            this.socket = socket;
            input = in;
        }

        private boolean doesContain(CopyOnWriteArrayList<Job> a, Job j){
            for(Job job : a){
                if(job.getId().equals(j.getId())){
                    return true;
                }
            }
            return false;
        }

        @Override
        public void run() {
            try {
                Job job = (Job)input.readObject();
                while (true) {
                    if (job.getJobType().equals("COMPLETED")) {
                        jobs.add(job);
                        break;
                    }
                    else if(doesContain(slaveAJobs, job)){
                        slaveAJobs.remove(job);
                        System.out.println("SLAVE-A completed " + job.toString());
                    }
                    else if(doesContain(slaveBJobs, job)){
                        slaveBJobs.remove(job);
                        System.out.println("SLAVE-B completed " + job.toString());
                    }
                    else {
                        jobs.add(job);
                        System.out.println("Received " + job.toString());
                    }
                    job = (Job) input.readObject();
                }

                System.out.println("FINISHED: InputThread");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Master master = new Master(5000);
    }
}
