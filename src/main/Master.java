package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class Master {
    public static final CopyOnWriteArrayList<Job> jobs = new CopyOnWriteArrayList<>();
    private HashMap<Socket, Object> activeClients = new HashMap<>();
    private static Socket socket;
    private final int port;
    static ObjectInputStream objectIn;
    static ObjectOutputStream objectOut;
//    static DataInputStream dataIn;
//    static DataOutputStream dataOut;

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
                objectIn = new ObjectInputStream(socket.getInputStream());
                Object input = objectIn.readObject();
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
        objectOut = new ObjectOutputStream(socket.getOutputStream());

        Thread t = new OutputThread(socket, activeClients, objectOut);

        t.start();
    }

    public void receiveMessage() {
        try {
            InputThread thread = new InputThread(socket, objectIn);
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
//                dataOut = new DataOutputStream(socket.getOutputStream());
//                dataIn = new DataInputStream(socket.getInputStream());
                while (true) {
                    synchronized (jobs) {
                        if (jobs.size() > 0) {
                            job = jobs.get(0);
                            Socket targetSocket = determineTargetSocket(Objects.requireNonNull(getTimesFromSlaves()), job);
//                            Slave slave = (Slave) activeClients.get(targetSocket);


                            if (job.getJobType().equals("Completed")) {
                                output.writeObject(job);
                                output.flush();
                                break;
                            }

                            if (targetSocket.equals(socket)) {
                                output.writeObject(job);
                                output.flush();
                                System.out.println("Sent job " + job.getId() + ", type " + job.getJobType());
                                jobs.remove(job);
                            }
                        }
                    }
                }


                System.out.println("FINISHED: OutputThread");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private HashMap<Integer, Socket> getTimesFromSlaves() {
            try {
                HashMap<Integer, Socket> slaveTimes = new HashMap<>();
                for (Socket s : activeClients.keySet()) {
                    if (activeClients.get(s) instanceof Slave) {

                        objectOut.write(1);
                        objectOut.flush();

                        slaveTimes.put(objectIn.readInt(), s);
                    }
                }

                return slaveTimes;
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }

        private Socket determineTargetSocket(HashMap<Integer, Socket> slaveTimes, Job job) {
            String jobType = job.getJobType();
            slaveTimes.forEach((key, value) -> {
                int timeAddition = 0;
                Slave slave = (Slave) activeClients.get(value);
                if (slave.getSlaveType().equals(jobType)) {
                    timeAddition = 2;
                } else {
                    timeAddition = 10;
                }
                slaveTimes.put((key + timeAddition), value);
            });

            int lowestTime = slaveTimes.keySet().stream().min(Comparator.naturalOrder()).orElse(0);
            return slaveTimes.get(lowestTime);
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
                Job job = (Job) obj;
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
