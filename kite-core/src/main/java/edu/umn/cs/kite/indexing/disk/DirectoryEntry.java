package edu.umn.cs.kite.indexing.disk;

/**
 * Created by amr_000 on 8/31/2016.
 */
public class DirectoryEntry {
    private String filePath;
    private String keyStr = null;
    private Integer keySpatialPartitionId = null;
    private long offset;
    private int length;

    public DirectoryEntry(String filePath, String key, long offset, int length)
    {
        this.filePath = filePath;
        this.keyStr = key;
        this.offset = offset;
        this.length = length;
    }

    public long getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public DirectoryEntry(String filePath, Integer keySpatialPartitionId,
                          long offset, int length) {
        this.filePath = filePath;
        this.keySpatialPartitionId = keySpatialPartitionId;
        this.offset = offset;
        this.length = length;
    }

    public String getFilePath() { return filePath; }
    public String getKeyStr() {
        return keyStr;
    }
    public Integer getKeyGeo() {
        return keySpatialPartitionId;
    }

    public String toString() {
        return keyStr+","+offset+","+length+","+filePath;
    }
}
