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

import com.facebook.presto.spi.block.ColumnarMap;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.format.ColumnMetaData;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MapColumnWriter
        implements ColumnWriter
{
    private final ColumnWriter keyWriter;
    private final ColumnWriter valueWriter;
    private final int maxDefinitionLevel;
    private final int maxRepetitionLevel;

    public MapColumnWriter(ColumnWriter keyWriter, ColumnWriter valueWriter, int maxDefinitionLevel, int maxRepetitionLevel)
    {
        this.keyWriter = requireNonNull(keyWriter);
        this.valueWriter = requireNonNull(valueWriter);
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.maxRepetitionLevel = maxRepetitionLevel;
    }

    @Override
    public void writeBlock(ColumnTrunk columnTrunk)
    {
        ColumnarMap columnarMap = ColumnarMap.toColumnarMap(columnTrunk.getBlock());

        ImmutableList<DefValueV2> defList = ImmutableList.<DefValueV2>builder().addAll(columnTrunk.getDefList()).add(DefValueV2.getIterator(columnarMap, maxDefinitionLevel)).build();
        ImmutableList<RepValueV2> repList = ImmutableList.<RepValueV2>builder().addAll(columnTrunk.getRepValueV2List()).add(RepValueV2.getIterator(columnTrunk.getBlock())).build();

        keyWriter.writeBlock(new ColumnTrunk(columnarMap.getKeysBlock(), null, null, defList, repList));
        valueWriter.writeBlock(new ColumnTrunk(columnarMap.getValuesBlock(), null, null, defList, repList));
    }

    @Override
    public void close()
    {
        keyWriter.close();
        valueWriter.close();
    }

    @Override
    public List<ColumnMetaData> getColumnMetaData()
    {
        ImmutableList.Builder<ColumnMetaData> builder = ImmutableList.builder();
        builder.addAll(keyWriter.getColumnMetaData());
        builder.addAll(valueWriter.getColumnMetaData());
        return builder.build();
    }

    @Override
    public List<ParquetDataOutput> getDataStreams()
    {
        ImmutableList.Builder<ParquetDataOutput> builder = ImmutableList.builder();
        builder.addAll(keyWriter.getDataStreams());
        builder.addAll(valueWriter.getDataStreams());
        return builder.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return keyWriter.getBufferedBytes() + valueWriter.getBufferedBytes();
    }

    @Override
    public void reset()
    {
        keyWriter.reset();
        valueWriter.reset();
    }
}

