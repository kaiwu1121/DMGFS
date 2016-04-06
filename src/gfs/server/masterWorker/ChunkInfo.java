//package gfs.server.master;

//import gfs.protocol.Chunk;


public class ChunkInfo extends Chunk {

    int seq; 
    String hash;
    private String[] doubles;
    Chunk chunk;
    INode inode;

    ChunkInfo(Chunk chk, int seq, String hash) {
        super(chk);
        this.seq = seq;
        this.hash = hash;
        this.doubles = new String[2];
        this.chunk = chk;
    }

    public void setInode(INode inode) {
        this.inode = inode;
    }

    
    public void setChunkServer(String[] triplet) {
        this.doubles = triplet;
    }

    public void removeServer(String str) {
        if (doubles[0].equals(str)) {
            doubles[0] = doubles[1];
        }
        doubles[1] = null;
    }

    public String[] getChunkServer() {
        return this.doubles;
    }

    public void setFirst(String str) {
        this.doubles[0] = str;
    }

    public String getFirst() {
        return this.doubles[0];
    }

    public void setSecond(String str) {
        this.doubles[1] = str;
    }

    public String getSecond() {
        return this.doubles[1];
    }
}
