//import gfs.protocol.Chunk;
import java.rmi.Remote;
import java.util.List;
import java.util.Map;


public interface MasterWorkerProtocol extends Remote {

    // add file
    public void addFile(String fileName) throws Exception;

    // ls file list
    public List fileList() throws Exception;

    // add chunk
    public List addChunk(String fileName, int seq, long size, String hash) throws Exception;

    // append
    public List getlastChunk(String fileName) throws Exception;

    // delete file
    public void deleteFile(String fileName) throws Exception;

    public void updateFile(String fileName, long size) throws Exception;

    // get all chunks of one file, including a back-up of chunk in map
    public Map<Chunk, String[]> getChunks(String fileName) throws Exception;

    // chunkserver reigster to master
    public void addServer(String ip) throws Exception;
    
   // public void removeServer(String ip) throws Exception;
    // chunkserversleep
    //
}
