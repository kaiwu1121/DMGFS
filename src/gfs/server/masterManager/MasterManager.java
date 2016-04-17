//package gfs.server.masterManager;

//import gfs.protocol.Chunk;
//import gfs.server.protocol.MasterManagerProtocol;
//import gfs.util.IPaddr;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;


public class MasterManager extends UnicastRemoteObject implements MasterManagerProtocol {

    String serverIP;
    private FSNamesystem namesystem;
    private Heartbeat hb;

    public MasterManager() throws Exception {
        //serverIP = "127.0.0.1";
        serverIP = IPaddr.getIP();
        namesystem = new FSNamesystem();
        hb = new Heartbeat();
    }

    // create new thred for heatbeat
    private class Heartbeat extends Thread {

        Heartbeat() {
            this.start();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    //heatbeat check every 30 seconds
                    sleep(1000 * 10);
                    namesystem.scan();
                }
            } catch (Exception ex) {
                System.out.println(ex);
            }
        }
    }

    @Override
    public String addServer(String ip) throws Exception {
        namesystem.addMasterWorkers(ip + ":9600");
        System.out.println("Server(masterworker) " + ip + " join in.");
        return serverIP;
    }
    
    @Override
    public void removeServer(String ip) throws Exception {
        namesystem.removeMasterWorkers(ip + ":9600");
        System.out.println("Server(masterworker) " + ip + " remove out.");
    }
    
    @Override
    public String gettargetServer(String ip) throws Exception {
        String targetServer = namesystem.getTargetMasterWorker(ip);
        System.out.println("Send " + ip + " to " + targetServer);
        return targetServer;
    }
   /* @Override
    public String addFile(String fileName, String ClientIP) throws Exception {
        String targetWorker = namesystem.addFile(fileName, ClientIp);
        System.out.println("File " + fileName + " added.");
       // String globeID = ClientIP + ":" + fileName;
        //String targetWorker = consistentHash.get(globeID);
       
        return targetWorker;
       // namesystem.addINode(fileName);
        //System.out.println("File " + fileName + " added.");
    }*/
    
/*
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
*/
    public static void main(String[] argv) throws Exception {
       
        MasterManager mastermanager = new MasterManager();
        LocateRegistry.createRegistry(9500);

        Naming.rebind("rmi://" + mastermanager.serverIP + ":9500/masterManager", mastermanager);
        System.out.println("MasterManager IP is " + mastermanager.serverIP);
    }
}
