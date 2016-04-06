//package gfs.server.protocol;

//import gfs.protocol.Chunk;
import java.rmi.*;
import java.util.List;
import java.util.Map;


public interface MasterManagerProtocol extends Remote {

    // upload file
   // public String addFile(String fileName, String ClientIP) throws Exception;

    // get file list
    //public List fileList() throws Exception;

    // add chunk
    //public List addChunk(String fileName, int seq, long size, String hash) throws Exception;

    //append
  //  public List getlastChunk(String fileName) throws Exception;

    // delete file
    //public void deleteFile(String fileName) throws Exception;

    //public void updateFile(String fileName, long size) throws Exception;

    // get all chunks of file, there is a back up of chunks included in map
  //  public Map<Chunk, String[]> getChunks(String fileName) throws Exception;

    // chunkserver Resiger to master
    public void addServer(String ip) throws Exception;
    public void removeServer(String ip) throws Exception;
    public String gettargetServer(String ip) throws Exception;
    // chunkserver sleep
    //
}
