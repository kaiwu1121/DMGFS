//package gfs.server.chunkserver;

//import gfs.protocol.Chunk;
//import gfs.server.protocol.ChunkServerProtocol;
//import gfs.server.protocol.MasterProtocol;
//import gfs.util.IPaddr;
//import gfs.util.Security;
import java.io.*;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class MasterWorker extends UnicastRemoteObject implements MasterWorkerProtocol {

    String serverIP;
    //List<Long> chunks;
   // Map<Long, String> chunkHash;
    MasterManagerProtocol mastermanager;
    //ChunkServerProtocol server;
   // String path = "data/chunkserver/";
    private FSNamesystem namesystem;
    private Heartbeat hb;

    public MasterWorker() throws Exception {
        serverIP = IPaddr.getIP();
       // chunks = new ArrayList();
        //chunkHash = new ConcurrentHashMap();

        mastermanager = (MasterManagerProtocol) Naming.lookup("rmi://192.168.1.102:9500/masterManager");
        mastermanager.addServer(serverIP);
       // server = null;
       // Chkcheck chkcheck = new Chkcheck();
        namesystem = new FSNamesystem();
        hb = new Heartbeat();
    }

    public MasterWorker(String socket) throws Exception {
        serverIP = IPaddr.getIP();
       // chunks = new ArrayList();
        //chunkHash = new ConcurrentHashMap();
        mastermanager = (MasterManagerProtocol) Naming.lookup("rmi://" + socket + ":9500/masterManager");
        mastermanager.addServer(serverIP);
      //  Chkcheck chkcheck = new Chkcheck();
        namesystem = new FSNamesystem();
        hb = new Heartbeat();
    }
/*
    @Override
    public Map<Long, String> hbCheck() throws Exception {
        return chunkHash;
    }*/

    // create new thred for heatbeat
    private class Heartbeat extends Thread {

        Heartbeat() {
            this.start();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    //heatbeat check every 60 seconds
                    sleep(1000 * 10);
                    namesystem.scan();
                }
            } catch (Exception ex) {
                System.out.println(ex);
            }
        }
    }

    @Override
    public void addServer(String ip) throws Exception {
        namesystem.addServer(ip + ":9600");
        System.out.println("Server(chunk server) " + ip + " join in.");
    }

    @Override
    public void addFile(String fileName) throws Exception {
        namesystem.addINode(fileName);
        System.out.println("File " + fileName + " added.");
    }

    @Override
    public List getlastChunk(String fileName) throws Exception {
        return namesystem.getlast(fileName);
    }

    @Override
    public void updateFile(String fileName, long size) throws Exception {
        namesystem.updateINode(fileName, size);
    }

    @Override
    public void deleteFile(String fileName) throws Exception {
        namesystem.deleteINode(fileName);
    }

    @Override
    public List fileList() throws Exception {
        return namesystem.fileList();
    }

    @Override
    public List addChunk(String fileName, int seq, long size, String hash) throws Exception {
        return namesystem.addChunk(fileName, seq, size, hash);
    }

    @Override
    public Map<Chunk, String[]> getChunks(String fileName) throws Exception {
        System.out.println("client request file: " + fileName);
        return namesystem.getChunks(fileName);
    }

    public static void main(String[] argv) throws Exception {
        MasterWorker masterworker;

        System.out.print("Please input masterManager ip: ");
        BufferedReader ipBuffer = new BufferedReader(new InputStreamReader(System.in));
        String masterManagerIP = ipBuffer.readLine();

        if (masterManagerIP.trim().length() != 0) {
            masterworker = new MasterWorker(masterManagerIP);
        } else {
            masterworker = new MasterWorker();
        }

        LocateRegistry.createRegistry(9600);
        Naming.rebind("rmi://" + masterworker.serverIP + ":9600/masterworker", masterworker);
    }
}

