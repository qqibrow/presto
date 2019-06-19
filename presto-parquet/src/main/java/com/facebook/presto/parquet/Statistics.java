package com.facebook.presto.parquet;

public class Statistics
{
    public int getRows()
    {
        return rows;
    }

    public long getBytes()
    {
        return bytes;
    }

    private final int rows;
    private final long bytes;

    public Statistics(int rows, long bytes)
    {
        this.rows = rows;
        this.bytes = bytes;
    }
}
