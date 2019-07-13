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

import com.facebook.presto.parquet.DefIteratorProvider.ColumnArrayIteratorProvider;
import com.facebook.presto.parquet.RepIteratorProvider.ArrayRepIterator;
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
        ColumnTrunk current = new ColumnTrunk(block,
                ImmutableList.<DefIteratorProvider>builder().addAll(columnTrunk.getDefList()).add(new ColumnArrayIteratorProvider(columnarArray, maxDefinitionLevel)).build(),
                ImmutableList.<RepIteratorProvider>builder().addAll(columnTrunk.getRepValueV2List()).add(new ArrayRepIterator(columnarArray, maxRepetitionLevel)).build());
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
