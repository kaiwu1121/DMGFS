//package gfs.server.master;

//import gfs.protocol.Chunk;
//import gfs.server.protocol.ChunkServerProtocol;
import java.rmi.Naming;
import java.util.*;


public class FSNamesystem {

    protected List<INode> inodes;
    protected List<String> servers;
    // all chunk on map
    protected Map<String, List<ChunkInfo>> serverInfo;
    protected static int current = 0;

    public FSNamesystem() {
        inodes = new ArrayList();
        servers = new ArrayList();
        serverInfo = new LinkedHashMap();
    }

    protected synchronized void addINode(String fileName) {
        // INode (file + chunkInfo[] chunk)
        INode inode = new INode(fileName);
        inodes.add(inode);
    }

    protected List fileList() {
        List<String> files = new ArrayList();
        Iterator iter = inodes.listIterator();
        while (iter.hasNext()) {
            INode inode = (INode) iter.next();
            files.add(inode.name);
        }
        return files;
    }

    protected synchronized List addChunk(String fileName, int seq,
            long length, String hash) {
        Calendar now = Calendar.getInstance();
        Date day = now.getTime();
        int time = (int) day.getTime();
        // use timestamp as chunk name
        String chkName = Integer.toString(time) + "0000"
                + Integer.toString(seq);
        Chunk chunk = new Chunk(Long.parseLong(chkName), length);
        // get chunkInfo of chun and set host machine 
        ChunkInfo chunkInfo = new ChunkInfo(chunk, seq, hash);
        System.out.println(hash);
        setServers(chunkInfo);
        // modify chunk information of inode
        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                inodes.get(i).addChunk(chunkInfo);
                chunkInfo.setInode(inodes.get(i));
                break;
            }
        }
        List result = new ArrayList();
        result.add(chunk);
        result.add(chunkInfo.getChunkServer());

        return result;
    }

    protected Map<Chunk, String[]> getChunks(String fileName)
            throws Exception {
        Map<Chunk, String[]> chunksMap = new LinkedHashMap();

        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                ChunkInfo[] chunks = inodes.get(i).chunks;
                for (int j = 0; j < chunks.length; j++) {
                    chunksMap.put(chunks[j].chunk,
                            chunks[j].getChunkServer());
                }
                break;
            }
        }
        return chunksMap;
    }

    protected List getlast(String fileName) throws Exception {
        List list = new ArrayList();
        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                ChunkInfo[] chunks = inodes.get(i).chunks;
                int j = chunks.length - 1;
                ChunkInfo chunkInfo = chunks[j];
                list.add(chunkInfo.chunk);
                list.add(chunkInfo.getChunkServer());
                list.add(j);
                break;
            }
        }
        return list;
    }

    protected void updateINode(String fileName, long size) throws Exception {
        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                ChunkInfo[] chunks = inodes.get(i).chunks;
                int j = chunks.length - 1;

                ChunkInfo chunkInfo = chunks[j];
                chunkInfo.chunk.setNumBytes(size);
                chunkInfo.setNumBytes(size);

                String socket = chunkInfo.getFirst();
                ChunkServerProtocol server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + socket + "/chunkServer");
                String hash = server.getUpdate(chunkInfo.chunk);
                chunkInfo.hash = hash;
                inodes.get(i).chunks[j] = chunkInfo;

                for (int k = 0; k < 2; k++) {
                    if (k == 0) {
                        socket = chunkInfo.getFirst();
                    } else {
                        socket = chunkInfo.getSecond();
                    }

                    List<ChunkInfo> chunklist = serverInfo.get(socket);
                    int index = chunklist.indexOf(chunkInfo);
                    chunklist.set(index, chunkInfo);
                    serverInfo.put(socket, chunklist);
                }
                break;
            }
        }
    }

    // manage chunkserver
    protected void addServer(String socket) {
        servers.add(socket);
    }

    protected void removeServer(String socket) {
        servers.remove(socket);
    }

    // distrbuted back-up server, load balance 
    protected void setServers(ChunkInfo chunkInfo) {
        List<ChunkInfo> chunks = new ArrayList();
        if (current == servers.size()) {
            current = 0;
        }
        chunkInfo.setFirst(servers.get(current));
        // process with serverInfo,add chunk information on chunkserver
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

    // using for Failure detection ,recover second back-up
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

    protected void deleteINode(String fileName) throws Exception {
        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                INode inode = inodes.get(i);
                inodes.remove(i);
                for (int j = 0; j < inode.chunks.length; j++) {
                    for (int k = 0; k < 2; k++) {
                        String socket;
                        if (k == 0) {
                            socket = inode.chunks[j].getFirst();
                        } else {
                            socket = inode.chunks[j].getSecond();
                        }

                        ChunkServerProtocol server = (ChunkServerProtocol) Naming.lookup(
                                "rmi://" + socket + "/chunkServer");
                        server.deleteChunk(inode.chunks[j].chunk);
                        List<ChunkInfo> chunks = serverInfo.get(socket);
                        chunks.remove(inode.chunks[j]);
                        serverInfo.put(socket, chunks);
                    }
                }
                System.out.println("file " + inode.getName() + " removed");
                break;
            }
        }
    }

    // test and process
    protected void scan() throws Exception {
        System.out.println("begin checking...");
        // return chunkserver data
        Map<Long, String> chkHash;
        // wrong data save
        Map<String, List<ChunkInfo>> failChunk = new LinkedHashMap();
        List<String> failServer = new LinkedList();

        ChunkServerProtocol server;
        // get crashed chunkserver and chunk
 /*           for (int i = 0; i < servers.size(); i++) {
            List<ChunkInfo> chunks = serverInfo.get(servers.get(i));
            Iterator iter = chunks.iterator();
            try {
                server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + servers.get(i) + "/chunkServer");
                chkHash = server.hbCheck();
               
                List<ChunkInfo> failure = new LinkedList();

                int num = 0;
            while (iter.hasNext()) {
                    ChunkInfo chunkInfo = (ChunkInfo) iter.next();
                    String hash = chkHash.get(chunkInfo.getChunkId());

                    if (hash == null || !hash.equals(chunkInfo.hash)) {
                        System.out.println("chunk " + chunkInfo.getChunkName() + " fault ");
                        
                        chunkInfo.removeServer(servers.get(i));
                        int idx = inodes.indexOf(chunkInfo.inode);
                        inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);
                        
                        serverInfo.get(servers.get(i)).set(num, chunkInfo);

                        failure.add(chunkInfo);
                    }
                    num++;
                }
               
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
               
                setServers(chunkInfo, chunkInfo.getFirst());
                int idx = inodes.indexOf(chunkInfo.inode);
                inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);

                server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + chunkInfo.getFirst() + "/chunkserver");
                server.backupChunk(chunkInfo.chunk, chunkInfo.getSecond());
            }
        }

       
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
        }*/
        System.out.println("check end, next check will after 20m");
    }
}
