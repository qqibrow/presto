package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;

import java.util.List;

public class LongColumnWriter
    implements ColumnWriter
{
    @Override
    public void writeBlock(Block block)
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    public List<ParquetDataOutput> getDataStreams()
    {
        return null;
    }

    @Override
    public long getBufferedBytes()
    {
        return 0;
    }

    @Override
    public long getRetainedBytes()
    {
        return 0;
    }

    @Override
    public void reset()
    {

    }
}
