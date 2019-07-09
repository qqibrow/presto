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

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class MetadataWriter
{
    public Slice getFooter(long totalRows, List<ColumnMetaData> stats)
    {
        return null;
//        FileMetaData fileMetaData = new FileMetaData();
//        fileMetaData.setVersion(1);
//
//        fileMetaData.setSchema(getSchema());
//        fileMetaData.setNum_rows(totalRows);
//
//        ColumnChunk columnChunk = new ColumnChunk(0);
//
//        Preconditions.checkArgument(stats.size() == 1, "stats size is not 1");
//        Statistics statistics = stats.get(0);
//        ColumnMetaData columnMetaData = new ColumnMetaData(
//                Type.INT64,
//                ImmutableList.of(Encoding.PLAIN),
//                ImmutableList.of("test_int_type"),
//                CompressionCodec.UNCOMPRESSED,
//                statistics.getRows(),
//                statistics.getBytes(),
//                statistics.getBytes(),
//                ParquetWriter.MAGIC.length);
//        columnChunk.setMeta_data(columnMetaData);
//        fileMetaData.setRow_groups(ImmutableList.of(new RowGroup(ImmutableList.of(columnChunk), statistics.getBytes(), statistics.getRows())));
//
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        try {
//            Util.writeFileMetaData(fileMetaData, outputStream);
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
//        return Slices.wrappedBuffer(outputStream.toByteArray());
    }

    List<SchemaElement> getSchema(List<Type> types, List<String> names)
    {
        ImmutableList.Builder<SchemaElement> list = ImmutableList.builder();

        SchemaElement root = new SchemaElement();
        root.setName("presto_schema");
        root.setNum_children(types.size());

        list.add(root);

        for (int i = 0; i < types.size(); i++) {
            SchemaElement child = new SchemaElement();
            child.setName(names.get(i));
            child.setType(ParquetWriterUtils.getParquetType(types.get(i)));
            child.setRepetition_type(FieldRepetitionType.OPTIONAL);
            list.add(child);
        }

        return list.build();
    }

    public Slice getFooter(List<RowGroup> rowGroups, List<SchemaElement> schemaElements)
    {
        FileMetaData fileMetaData = new FileMetaData();
        fileMetaData.setVersion(1);
        fileMetaData.setSchema(schemaElements);
        long totalRows = rowGroups.stream().mapToLong(RowGroup::getNum_rows).sum();
        fileMetaData.setNum_rows(totalRows);
        fileMetaData.setRow_groups(ImmutableList.copyOf(rowGroups));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            Util.writeFileMetaData(fileMetaData, outputStream);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return Slices.wrappedBuffer(outputStream.toByteArray());
    }
}
