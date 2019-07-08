package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarRow;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.Preconditions;
import org.apache.parquet.format.ColumnMetaData;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class StructColumnWriter
        implements ColumnWriter
{
    private final List<ColumnWriter> structFields;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;

    public StructColumnWriter(List<ColumnWriter> structFields, int maxDefinitionLevel, int maxRepetitionLevel)
    {
        this.structFields = requireNonNull(structFields);
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.maxRepetitionLevel = maxRepetitionLevel;
    }

    @Override
    public void writeBlock(ColumnTrunk columnTrunk)
    {
        ColumnarRow columnarRow = ColumnarRow.toColumnarRow(columnTrunk.getBlock());
        Preconditions.checkArgument(columnarRow.getFieldCount() == structFields.size(), "");
        for (int i = 0; i < structFields.size(); ++i) {
            ColumnWriter columnWriter = structFields.get(i);
            Block block = columnarRow.getField(i);
            ColumnTrunk current;

            // TODO this is incorrect?
            if (!columnTrunk.getDefIterator().hasNext()) {
                current = new ColumnTrunk(block,
                        new DefinitionValueIterator.RowIterator(columnarRow, maxDefinitionLevel),
                        new RepetitionValueIterator.BlockIterator(columnTrunk.getBlock()));
            }
            else {
                current = new ColumnTrunk(block,
                        new DefinitionValueIterator.RowIterator(columnTrunk.getDefIterator(), columnarRow, maxDefinitionLevel),
                        new RepetitionValueIterator.BlockIterator(columnTrunk.getRepIterator(), columnTrunk.getBlock()));
            }
            columnWriter.writeBlock(current);
        }
    }

    @Override
    public void close()
    {
        structFields.forEach(ColumnWriter::close);
    }

    @Override
    public List<ColumnMetaData> getColumnMetaData()
    {
        ImmutableList.Builder<ColumnMetaData> builder = ImmutableList.builder();
        structFields.stream().map(ColumnWriter::getColumnMetaData).forEach(builder::addAll);
        return builder.build();
    }

    @Override
    public List<ParquetDataOutput> getDataStreams()
    {
        ImmutableList.Builder<ParquetDataOutput> builder = ImmutableList.builder();
        structFields.stream().map(ColumnWriter::getDataStreams).forEach(builder::addAll);
        return builder.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return structFields.stream().mapToLong(ColumnWriter::getBufferedBytes).sum();
    }

    @Override
    public void reset()
    {
        structFields.forEach(ColumnWriter::reset);
    }
}
