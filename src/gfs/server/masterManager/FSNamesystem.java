//package gfs.server.masterManager;

//import gfs.protocol.Chunk;
//import gfs.server.protocol.ChunkServerProtocol;
import java.rmi.Naming;
import java.util.*;


public class FSNamesystem {
   protected ConsistentHash<String> newWorker;
   protected HashSet<String> masterworkers;
    //protected List<INode> inodes;
  //  protected List<String> servers;
    // chunk on each server
    //protected Map<String, List<ChunkInfo>> serverInfo;
    protected static int current = 0;

    public FSNamesystem() {
        masterworkers = new HashSet<String>();
        //hash fucntion + # of virtual node + mapping relationship of virtual nodes hash value to masterworker 
        newWorker = new ConsistentHash<String>(new HashFunction(), 120, masterworkers);
        //inodes = new ArrayList();
        //servers = new ArrayList();
        //serverInfo = new LinkedHashMap();
    }

    protected synchronized void addMasterWorkers(String socket) {
        // INode (file + chunkInfo[] chunk)
        newWorker.add(socket);
        System.out.println(newWorker.getSize());
        masterworkers.add(socket);
    }
 
    protected void removeMasterWorkers(String socket) {
        newWorker.remove(socket);
        masterworkers.remove(socket);
    }

    protected synchronized String getTargetMasterWorker(String socket) {
        // INode (file + chunkInfo[] chunk)
        String targetMasterWorker = newWorker.get(socket);
       // System.out.println(targetMasterWorker);
        //masterworkers.add(socket);
        return targetMasterWorker;
    }
   
    // heatbeat
     protected void scan() throws Exception {
         System.out.println("begin checking...");
         System.out.println(newWorker.getSize());
     }
    

}
