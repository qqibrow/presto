package com.facebook.presto.parquet;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
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

    List<SchemaElement> getSchema()
    {
        SchemaElement root = new SchemaElement();
        root.setName("presto_schema");
        root.setNum_children(1);

        SchemaElement child = new SchemaElement();
        child.setName("test_int_type");
        child.setType(Type.INT64);
        child.setConverted_type(ConvertedType.INT_64);
        child.setRepetition_type(FieldRepetitionType.REQUIRED);
        return ImmutableList.of(root, child);
    }

    public Slice getFooter(List<RowGroup> rowGroups)
    {
        FileMetaData fileMetaData = new FileMetaData();
        fileMetaData.setVersion(1);
        fileMetaData.setSchema(getSchema());
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
