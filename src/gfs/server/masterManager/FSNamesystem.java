package gfs.server.masterManager;

import gfs.protocol.Chunk;
import gfs.server.protocol.ChunkServerProtocol;
import java.rmi.Naming;
import java.util.*;


public class FSNamesystem {
    
    protected List<Masterworker> masterworkers;
    //protected List<INode> inodes;
    protected List<String> servers;
    // 每一个server上面对应的chunk
    //protected Map<String, List<ChunkInfo>> serverInfo;
    protected static int current = 0;

    public FSNamesystem() {
        masterworkers = new HashSet<String>();
        //hash fucntion + # of virtual node + mapping relationship of virtual nodes hash value to masterworker 
        ConsistentHash<String> consistentHash = new ConsistentHash<String>(new HashFunction(), 2, masterworkers);
        //inodes = new ArrayList();
        //servers = new ArrayList();
        //serverInfo = new LinkedHashMap();
    }

    protected synchronized void addMasterWorkers(String fileName) {
        // INode (file + chunkInfo[] chunk)
        masterworkers.add(socket);
    }
 
    protected void removeMasterWorkers(String socket) {
        masterworkers.remove(socket);
    }
    
    // 管理chunkserver
    protected void addServer(String socket) {
        servers.add(socket);
    }

    protected void removeServer(String socket) {
        servers.remove(socket);
    }

    // 分配备份服务器，此处可做负载均衡
    protected void setServers(ChunkInfo chunkInfo) {
        List<ChunkInfo> chunks = new ArrayList();
        if (current == servers.size()) {
            current = 0;
        }
        chunkInfo.setFirst(servers.get(current));
        // 处理serverInfo,对chunkserver添加chunk信息
        if (serverInfo.get(servers.get(current)) != null) {
            chunks = serverInfo.get(servers.get(current));
        }
        chunks.add(chunkInfo);
        serverInfo.put(servers.get(current), chunks);
        current++;
        chunks = new ArrayList();

        if (current == servers.size()) {
            current = 0;
        }
        chunkInfo.setSecond(servers.get(current));
        if (serverInfo.get(servers.get(current)) != null) {
            chunks = serverInfo.get(servers.get(current));
        }
        chunks.add(chunkInfo);
        serverInfo.put(servers.get(current), chunks);
        current++;
    }

    // 用于检错，恢复第二个备份
    protected void setServers(ChunkInfo chunkInfo, String server) {
        List<ChunkInfo> chunks = new ArrayList();
        if (current == servers.indexOf(server)) {
            current++;
        }
        if (current >= servers.size()) {
            current = 0;
        }

        chunkInfo.setSecond(servers.get(current));
        if (serverInfo.get(servers.get(current)) != null) {
            chunks = serverInfo.get(servers.get(current));
        }
        chunks.add(chunkInfo);
        serverInfo.put(servers.get(current), chunks);
        current++;
    }


    // 检测并处理
    protected void scan() throws Exception {
        System.out.println("begin checking...");
        // chunkserver返回数据
        Map<Long, String> chkHash;
        // 出错数据保存
        Map<String, List<ChunkInfo>> failChunk = new LinkedHashMap();
        List<String> failServer = new LinkedList();

        ChunkServerProtocol server;
        // 获取出错chunkserver及 出错chunk
        for (int i = 0; i < servers.size(); i++) {
            List<ChunkInfo> chunks = serverInfo.get(servers.get(i));
            Iterator iter = chunks.iterator();
            try {
                server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + servers.get(i) + "/chunkserver");
                chkHash = server.hbCheck();
                // 临时数组，用于添加失效chunk
                List<ChunkInfo> failure = new LinkedList();

                int num = 0;
                while (iter.hasNext()) {
                    ChunkInfo chunkInfo = (ChunkInfo) iter.next();
                    String hash = chkHash.get(chunkInfo.getChunkId());

                    if (hash == null || !hash.equals(chunkInfo.hash)) {
                        System.out.println("chunk " + chunkInfo.getChunkName() + " fault ");
                        // 修改namespace中chunkinfo信息,inode中信息
                        chunkInfo.removeServer(servers.get(i));
                        int idx = inodes.indexOf(chunkInfo.inode);
                        inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);
                        // 修改serverInfo信息
                        serverInfo.get(servers.get(i)).set(num, chunkInfo);

                        failure.add(chunkInfo);
                    }
                    num++;
                }
                // 由于分配chunk都需要通过master，所以遍历一次后，chunkserver不会有剩余的chunk
                failChunk.put(servers.get(i), failure);
            } catch (Exception ex) {
                System.out.println("server " + servers.get(i) + " down!");

                while (iter.hasNext()) {
                    ChunkInfo chunkInfo = (ChunkInfo) iter.next();
                    chunkInfo.removeServer(servers.get(i));

                    int idx = inodes.indexOf(chunkInfo.inode);
                    inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);
                    serverInfo.remove(servers.get(i));
                }
                failServer.add(servers.get(i));
            }
        }

        // 处理出错
        // 1. chunkserver失效
        Iterator iter = failServer.iterator();
        while (iter.hasNext()) {
            String socket = (String) iter.next();
            System.out.println("backup fault server " + socket + " ...");

            List<ChunkInfo> chunks = serverInfo.get(socket);
            Iterator chunkIter = chunks.iterator();
            while (chunkIter.hasNext()) {
                ChunkInfo chunkInfo = (ChunkInfo) chunkIter.next();
                System.out.println("backup chunk " + chunkInfo.getChunkName()
                        + " of fault server " + socket);
                // 节点分配
                setServers(chunkInfo, chunkInfo.getFirst());
                int idx = inodes.indexOf(chunkInfo.inode);
                inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);

                server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + chunkInfo.getFirst() + "/chunkserver");
                server.backupChunk(chunkInfo.chunk, chunkInfo.getSecond());
            }
        }

        // 2. chunkserver个别chunk失效，直接复制到该主机上
        Iterator mapIter = failChunk.entrySet().iterator();
        while (mapIter.hasNext()) {
            Map.Entry entry = (Map.Entry) mapIter.next();
            String socket = (String) entry.getKey();
            List<ChunkInfo> chunks = (List) entry.getValue();

            Iterator chunkIter = chunks.iterator();
            while (chunkIter.hasNext()) {
                ChunkInfo chunkInfo = (ChunkInfo) chunkIter.next();
                System.out.println("backup fault chunk "
                        + chunkInfo.getChunkName() + " on server " + socket);
                chunkInfo.setSecond(socket);
                int idx = inodes.indexOf(chunkInfo.inode);
                inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);

                server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + chunkInfo.getFirst() + "/chunkserver");
                server.backupChunk(chunkInfo.chunk, socket);
            }
        }
        System.out.println("check end, next check will after 20m");
    }
}
