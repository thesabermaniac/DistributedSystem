package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Master {
    public static final CopyOnWriteArrayList<Job> jobs = new CopyOnWriteArrayList<>();
    private HashMap<Socket, Object> activeClients = new HashMap<>();
    private static Socket socket;
    private final int port;
    static ObjectInputStream in;
    static ObjectOutputStream out;

    public Master(int port) throws IOException {
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

            while (true) {
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
        out = new ObjectOutputStream(socket.getOutputStream());

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

    static class OutputThread extends Thread {
        final Socket socket;
        HashMap<Socket, Object> activeClients;
        ObjectOutputStream output;

        public OutputThread(Socket s, HashMap<Socket, Object> clients, ObjectOutputStream out) {
            activeClients = clients;
            socket = s;
            output = out;
        }

        @Override
        public void run() {
            try {
                Job job;
                while (true) {
                    synchronized (jobs) {
                        if (jobs.size() > 0) {
                            if (activeClients.get(socket) instanceof Slave) {
                                job = jobs.get(0);
                                Slave slave = (Slave) activeClients.get(socket);

                                if (job.getJobType().equals("Completed")) {
                                    output.writeObject(job);
                                    output.flush();
                                    break;
                                }

                                if (slave.getSlaveType().equals(job.getJobType())) {
                                    output.writeObject(job);
                                    output.flush();
                                    System.out.println("Sent job " + job.getId() + ", type " + job.getJobType());
                                    jobs.remove(job);
                                }
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

        @Override
        public void run() {
            try {
                Object obj = input.readObject();
                System.out.println(obj);
                Job job = (Job)obj;
                while (true) {
                    jobs.add(job);
                    System.out.println("Received job " + job.getId());
                    job = (Job) input.readObject();
                    if (job.getJobType().equals("Completed")) {
                        jobs.add(job);
                        break;
                    }
                }

                System.out.println("FINISHED: InputThread");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Master master = new Master(5000);
    }
}
