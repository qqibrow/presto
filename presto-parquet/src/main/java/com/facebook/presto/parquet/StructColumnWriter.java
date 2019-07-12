/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
            ColumnTrunk current = new ColumnTrunk(block,
                    ImmutableList.<DefValueV2>builder().addAll(columnTrunk.getDefList()).add(DefValueV2.getIterator(columnarRow, maxDefinitionLevel)).build(),
                    ImmutableList.<RepValueV2>builder().addAll(columnTrunk.getRepValueV2List()).add(RepValueV2.getIterator(columnTrunk.getBlock())).build());
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
