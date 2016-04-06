//package gfs.protocol;

import java.io.Serializable;


public class Chunk implements Serializable {

   
    private long chunkId;
    private long numBytes;

    public Chunk(long chkid, long length) {
        this.chunkId = chkid;
        this.numBytes = length;
    }

    public Chunk(Chunk chk) {
        this.chunkId = chk.chunkId;
        this.numBytes = chk.numBytes;
    }

    public long getChunkId() {
        return chunkId;
    }

    public void setChunkId(long chkid) {
        chunkId = chkid;
    }

    public long getNumBytes() {
        return numBytes;
    }

    public void setNumBytes(long len) {
        numBytes = len;
    }

    
    public String getChunkName() {
        return "chk_" + String.valueOf(chunkId);
    }
}
