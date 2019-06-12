package com.facebook.presto.parquet;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static java.util.Objects.requireNonNull;

public interface ParquetDataOutput
{
    static ParquetDataOutput createDataOutput(Slice slice)
    {
        requireNonNull(slice, "slice is null");
        return new ParquetDataOutput()
        {
            @Override
            public long size()
            {
                return slice.length();
            }

            @Override
            public void writeData(SliceOutput sliceOutput)
            {
                sliceOutput.writeBytes(slice);
            }
        };
    }

    /**
     * Number of bytes that will be written.
     */
    long size();

    /**
     * Writes data to the output. The output must be exactly
     * {@link #size()} bytes.
     */
    void writeData(SliceOutput sliceOutput);
}
