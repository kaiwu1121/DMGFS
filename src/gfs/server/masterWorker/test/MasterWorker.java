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

    public MasterWorker() throws Exception {
        serverIP = IPaddr.getIP();
       // chunks = new ArrayList();
        //chunkHash = new ConcurrentHashMap();

        mastermanager = (MasterManagerProtocol) Naming.lookup("rmi://192.168.1.102:9500/masterManager");
        mastermanager.addServer(serverIP);
       // server = null;
       // Chkcheck chkcheck = new Chkcheck();
    }

    public MasterWorker(String socket) throws Exception {
        serverIP = IPaddr.getIP();
       // chunks = new ArrayList();
        //chunkHash = new ConcurrentHashMap();
        mastermanager = (MasterManagerProtocol) Naming.lookup("rmi://" + socket + ":9500/masterManager");
        mastermanager.addServer(serverIP);
      //  Chkcheck chkcheck = new Chkcheck();
    }
/*
    @Override
    public Map<Long, String> hbCheck() throws Exception {
        return chunkHash;
    }*/

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
        Naming.rebind("rmi://" + masterworker.serverIP + ":9600/chunkserver", masterworker);
    }
}