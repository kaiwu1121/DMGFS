//package gfs;

//import gfs.protocol.Chunk;
//import gfs.server.protocol.ChunkServerProtocol;
//import gfs.server.protocol.MasterProtocol;
//import gfs.util.Security;
import java.io.*;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class DFS {

    // default chunk size
    private final static int defSize = 1024 * 1024 * 7;
    private String serverIP;
    private MasterManagerProtocol MasterManager;
    private MasterWorkerProtocol MasterWorker;
  //  private MasterProtocol master;
    private ChunkServerProtocol server;
    private String path = "C:\\Users\\kaiwu\\Documents\\GitHub\\DMGFS\\src\\gfs\\client\\classes\\data";

    // init
    public DFS(String socket) throws Exception {
         serverIP = IPaddr.getIP();
         MasterManager = (MasterManagerProtocol) Naming.lookup("rmi://" + socket + "/masterManager");
         String targetMasterWorker = MasterManager.gettargetServer(serverIP);
         System.out.println(targetMasterWorker);
         MasterWorker = (MasterWorkerProtocol) Naming.lookup("rmi://" + targetMasterWorker + "/masterWorker");
        
        
      //  master = (MasterProtocol) Naming.lookup("rmi://" + socket + "/master");
        server = null;
    }

    // upload file
    public void upload(String fileName) throws Exception {
        // add inode
        MasterWorker.addFile(fileName);

        String hash;
        int n, seq = 0;
        byte[] buffer = new byte[defSize];

       // InputStream input = new FileInputStream(path + fileName);
        InputStream input = new FileInputStream("data\\fix_bugs_list.txt");
        input.skip(0);
        while ((n = input.read(buffer, 0, defSize)) > 0) {
            byte[] upbytes = new byte[n];
            System.arraycopy(buffer, 0, upbytes, 0, n);
            hash = Security.getMD5sum(upbytes);
            uploadChunk(fileName, upbytes, seq, n, hash);
            seq++;
        }
        input.close();
    }

    public void delete(String fileName) throws Exception {
        MasterWorker.deleteFile(fileName);
    }

    // upload one chunk
    public void uploadChunk(String fileName, byte[] stream, int seq, long size, String hash)
            throws Exception {
        // get its corresponding chunkserver 
        List list = (ArrayList) MasterWorker.addChunk(fileName, seq, size, hash);
        Chunk chunk = (Chunk) list.get(0);
        String[] sockets = (String[]) list.get(1);

        server = (ChunkServerProtocol) Naming.lookup("rmi://" + sockets[0] + "/chunkserver");
        server.saveChunk(chunk, stream, sockets[1]); //server list for data deliver
    }

    public void updateFile(String fileName, byte[] stream) throws Exception {
        List list = MasterWorker.getlastChunk(fileName);
        Chunk chunk = (Chunk) list.get(0);
        String[] sockets = (String[]) list.get(1);
        int seq = (Integer) list.get(2);
        seq++;

        int ptr = 0;
        int lastleng = (int) chunk.getNumBytes();
        int streamleng = stream.length;

        ptr = defSize - lastleng;
        if (ptr >= streamleng) {
            byte[] upbytes = new byte[streamleng];
            System.arraycopy(stream, 0, upbytes, 0, streamleng);
            server = (ChunkServerProtocol) Naming.lookup("rmi://" + sockets[0] + "/chunkserver");
            server.updateChunk(chunk, upbytes, sockets[1]); //server list for data deliver
            MasterWorker.updateFile(fileName, lastleng + streamleng);
        } else {
            byte[] upbytes = new byte[ptr];
            System.arraycopy(stream, 0, upbytes, 0, ptr);
            String hash = Security.getMD5sum(upbytes);
            server = (ChunkServerProtocol) Naming.lookup("rmi://" + sockets[0] + "/chunkserver");
            server.updateChunk(chunk, upbytes, sockets[1]); //server list for data deliver
            //uploadChunk(fileName, upbytes, seq, ptr, hash); 
            MasterWorker.updateFile(fileName, defSize);
            while (ptr + defSize <= streamleng) {
                upbytes = new byte[defSize];
                System.arraycopy(stream, ptr, upbytes, 0, defSize);
                hash = Security.getMD5sum(upbytes);
                uploadChunk(fileName, upbytes, seq, defSize, hash);
                ptr += defSize;
                seq++;
            }
            if (ptr < streamleng) {
                int lastSize = streamleng - ptr;
                upbytes = new byte[lastSize];
                System.arraycopy(stream, ptr, upbytes, 0, lastSize);
                hash = Security.getMD5sum(upbytes);
                uploadChunk(fileName, upbytes, seq, lastSize, hash);
            }
        }
//        if (lastleng < defSize) {
//            ptr = defSize - lastleng;
//            if (ptr >= streamleng) {
//                byte[] upbytes = new byte[streamleng];
//                System.arraycopy(stream, 0, upbytes, 0, streamleng);
//                server = (ChunkServerProtocol) Naming.lookup("rmi://" + sockets[0] + "/chunkserver");
//                server.saveChunk(chunk, upbytes, sockets[1]); //server list for data deliver
//                
//                master.updateFile(fileName, lastleng + streamleng);
//            }
//            
//        }
//        while(ptr < streamleng){
//            if(ptr + defSize <= streamleng) {
//                byte[] upbytes = new byte[defSize];
//                System.arraycopy(stream, ptr, upbytes, 0, defSize);
//                String hash = Security.getMD5sum(upbytes);
//                uploadChunk(fileName, upbytes, seq, defSize, hash);
//                
//            } else {
//                int leng = streamleng - ptr;
//                byte[] upbytes = new byte[leng];
//                System.arraycopy(stream, ptr, upbytes, 0, leng);
//                String hash = Security.getMD5sum(upbytes);
//                uploadChunk(fileName, upbytes, seq, leng, hash);
//                master.updateFile(fileName, defSize);
//            }
//            seq++;
//            ptr += defSize;
//        }
    }

    //  get file list from server
    public List getFileList() throws Exception {
        List list = (ArrayList) MasterWorker.fileList();
        return list;
    }

    // download a file
    public void download(String fileName) throws Exception {
        File getFile = new File(path + "new_" + fileName);
        OutputStream output = new FileOutputStream(getFile);

        Map<Chunk, String[]> chunks = (Map) MasterWorker.getChunks(fileName);

        Iterator iter = chunks.entrySet().iterator();
        int off = 0, length = 0;
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Chunk chunk = (Chunk) entry.getKey();
            String servers[] = (String[]) entry.getValue();
            String socket = servers[0];

            output.write(downloadChunk(chunk, socket));
        }
        output.close();
    }

    public byte[] downloadChunk(Chunk chunk, String socket) throws Exception {
        server = (ChunkServerProtocol) Naming.lookup("rmi://" + socket + "/chunkserver");

        return server.getChunk(chunk);
    }
}