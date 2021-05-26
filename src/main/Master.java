package main;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class Master {
    public static final CopyOnWriteArrayList<Job> jobs = new CopyOnWriteArrayList<>();
    private final HashMap<Socket, Object> activeClients = new HashMap<>();
    private final HashMap<Socket, ObjectOutputStream> clientsOutputStreams = new HashMap<>();
    private final HashMap<Socket, Integer> slaveTimes = new HashMap<>();
    private Socket socket;
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
            boolean firstSlave = true;
            while (true) {
                socket = serverSocket.accept();

                System.out.println("New client connected");
                objectIn = new ObjectInputStream(socket.getInputStream());
                objectOut = new ObjectOutputStream(socket.getOutputStream());
                Object input = objectIn.readObject();
                if (input instanceof Slave) {
                    Slave slave = (Slave) input;
                    activeClients.put(socket, slave);
                    clientsOutputStreams.put(socket, objectOut);
                    new SlaveListeningThread(socket, objectIn).start();
                    if (firstSlave) {
                        firstSlave = false;
                        new JobSendingThread().start();
                    }
                    System.out.println("Socket: " + socket + ", slave: " + slave);
                } else {
                    Client client = (Client) input;
                    activeClients.put(socket, client);
                    clientsOutputStreams.put(socket, objectOut);
                    new ClientCommunicationThread(objectIn).start();
                    System.out.println("Socket: " + socket + ", client: " + client);
                }

//                delegate();
//                receiveMessage();
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

    class SlaveListeningThread extends Thread {
        Socket socket;
        ObjectInputStream inputStream;

        SlaveListeningThread(Socket socket, ObjectInputStream inputStream) {
            this.socket = socket;
            this.inputStream = inputStream;
        }

        @Override
        public void run() {
            try {
                Object receivedObject;
                while (true) {
                    receivedObject = objectIn.readObject();

                    if (receivedObject instanceof Job) {
                        if (((Job) receivedObject).getJobType().equals("Completed")) {
                            break;
                        }
//                        new JobSendingThread((Job) receivedObject).start();
                    }
                    if (receivedObject instanceof Integer) {
                        System.out.println("GOT NUMBER");
                        slaveTimes.put(socket, (Integer) receivedObject);
                    }
//                    objectIn.reset();
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    class ClientCommunicationThread extends Thread {
        ObjectInputStream inputStream;

        ClientCommunicationThread(ObjectInputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Job job = (Job) inputStream.readObject();
                    if (job.getJobType().equals("Completed")) {
                        break;
                    }
                    synchronized (jobs) {
                        jobs.add(job);
                    }
                    if (!jobs.isEmpty()) {
                        System.out.println("Received job " + job.getId());
                    }
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class JobSendingThread extends Thread {
//        Job job;
//
//        JobSendingThread(Job job) {
//            this.job = job;
//        }

        @Override
        public void run() {
            while (true) {
                if (!jobs.isEmpty()) {
                    Job job = jobs.get(0);
                    askSlavesForTimes();
                    while (true) {
                        try {
                            Thread.sleep(0);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        synchronized (slaveTimes) {
                            if (slaveTimes.size() >= 2) {
                                break;
                            }
                        }
                    }
                    Socket targetSocket = determineTargetSocket(job);

                    try {
                        synchronized (clientsOutputStreams) {
                            clientsOutputStreams.get(targetSocket).writeObject(job);
                            clientsOutputStreams.get(targetSocket).flush();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    synchronized (jobs) {
                        jobs.remove(0);
                    }
                    System.out.println("Job sent!");
                }
            }
        }

        private void askSlavesForTimes() {
            try {
                synchronized (clientsOutputStreams) {
                    synchronized (activeClients) {
                        for (Socket s : activeClients.keySet()) {
                            if (activeClients.get(s) instanceof Slave) {

                                clientsOutputStreams.get(s).writeObject(1);
                                clientsOutputStreams.get(s).flush();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private Socket determineTargetSocket(Job job) {
            String jobType = job.getJobType();

//            while (slaveTimes.size() < 2);

            synchronized (slaveTimes) {
                slaveTimes.forEach((key, value) -> {
                    int timeAddition;
                    Slave slave = (Slave) activeClients.get(key);
                    if (slave.getSlaveType().equals(jobType)) {
                        timeAddition = 2;
                    } else {
                        timeAddition = 10;
                    }
                    slaveTimes.put(key, (value + timeAddition));
                });

                int lowestTime = slaveTimes.values().stream().min(Comparator.naturalOrder()).orElse(0);

                Socket targetSocket = null;
                for (Map.Entry<Socket, Integer> entry : slaveTimes.entrySet()) {
                    Socket key = entry.getKey();
                    Integer value = entry.getValue();
                    if (value == lowestTime) {
                        targetSocket = key;
                    }
                }

                slaveTimes.clear();

//                return (Socket) slaveTimes.entrySet().stream().filter(e -> e.getValue() == lowestTime);
                return targetSocket;
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
        final HashMap<Socket, Object> activeClients;
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
//                            Socket targetSocket = determineTargetSocket(Objects.requireNonNull(getTimesFromSlaves()), job);
////                            Slave slave = (Slave) activeClients.get(targetSocket);
//
//                            if (job.getJobType().equals("Completed")) {
//                                output.writeObject(job);
//                                output.flush();
//                                break;
//                            }
//
//                            System.out.println("target: " + targetSocket);
//                            System.out.println("Socket: " + socket);
//                            if (targetSocket.equals(socket)) {
//                                output.writeObject(job);
//                                output.flush();
//                                System.out.println("Sent job " + job.getId() + ", type " + job.getJobType());
//                                jobs.remove(job);
//                            }
                        }
                    }
                }


//                System.out.println("FINISHED: OutputThread");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

//        private HashMap<Integer, Socket> getTimesFromSlaves() {
//            try {
//                HashMap<Integer, Socket> slaveTimes = new HashMap<>();
//                synchronized (activeClients) {
//                    for (Socket s : activeClients.keySet()) {
//                        if (activeClients.get(s) instanceof Slave) {
//
//                            objectOut.writeObject(1);
//                            objectOut.flush();
//
//                            slaveTimes.put(objectIn.readInt(), s);
//                        }
//                    }
//                }
//
//                return slaveTimes;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            return null;
//        }

        private Socket determineTargetSocket(HashMap<Socket, Integer> slaveTimes, Job job) {
            String jobType = job.getJobType();
            slaveTimes.forEach((key, value) -> {
                int timeAddition;
                Slave slave = (Slave) activeClients.get(key);
                if (slave.getSlaveType().equals(jobType)) {
                    timeAddition = 2;
                } else {
                    timeAddition = 10;
                }
                slaveTimes.put(key, (value + timeAddition));
            });

            int lowestTime = slaveTimes.values().stream().min(Comparator.naturalOrder()).orElse(0);
            return (Socket) slaveTimes.entrySet().stream().filter(e -> e.getValue() == lowestTime);
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
