package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.format.ColumnMetaData;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ArrayColumnWriter
        implements ColumnWriter
{
    private final ColumnWriter elementWriter;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;

    public ArrayColumnWriter(ColumnWriter elementWriter, int maxDefinitionLevel, int maxRepetitionLevel)
    {
        this.elementWriter = requireNonNull(elementWriter);
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.maxRepetitionLevel = maxRepetitionLevel;
    }

    @Override
    public void writeBlock(ColumnTrunk columnTrunk)
    {
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(columnTrunk.getBlock());
        Block block = columnarArray.getElementsBlock();
        ColumnTrunk current;

        if (!columnTrunk.getDefIterator().hasNext()) {
            current = new ColumnTrunk(block,
                    new DefinitionValueIterator.ArrayIterator(columnarArray, maxDefinitionLevel),
                    new RepetitionValueIterator.ArrayIterator(columnarArray));
        }
        else {
            current = new ColumnTrunk(block,
                    new DefinitionValueIterator.ArrayIterator(columnTrunk.getDefIterator(), columnarArray, maxDefinitionLevel),
                    new RepetitionValueIterator.ArrayIterator(columnTrunk.getRepIterator(), columnarArray, maxRepetitionLevel));
        }
        elementWriter.writeBlock(current);
    }

    @Override
    public void close()
    {
        elementWriter.close();
    }

    @Override
    public List<ColumnMetaData> getColumnMetaData()
    {
        ImmutableList.Builder<ColumnMetaData> builder = ImmutableList.builder();
        builder.addAll(elementWriter.getColumnMetaData());
        return builder.build();
    }

    @Override
    public List<ParquetDataOutput> getDataStreams()
    {
        ImmutableList.Builder<ParquetDataOutput> builder = ImmutableList.builder();
        builder.addAll(elementWriter.getDataStreams());
        return builder.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return elementWriter.getBufferedBytes();
    }

    @Override
    public void reset()
    {
        elementWriter.reset();
    }
}
