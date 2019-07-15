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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.OrcFileWriterConfig;
import com.facebook.presto.hive.ParquetFileWriterConfig;
import com.facebook.presto.hive.benchmark.FileFormat;
import com.facebook.presto.hive.parquet.write.MapKeyValuesSchemaConverter;
import com.facebook.presto.hive.parquet.write.SingleLevelArrayMapKeyValuesSchemaConverter;
import com.facebook.presto.hive.parquet.write.SingleLevelArraySchemaConverter;
import com.facebook.presto.hive.parquet.write.TestMapredParquetOutputFormat;
import com.facebook.presto.parquet.ParquetWriter;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.hive.AbstractTestHiveFileFormats.getFieldFromCursor;
import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.facebook.presto.hive.HiveUtil.isRowType;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.RowType.field;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Functions.constant;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static parquet.hadoop.ParquetOutputFormat.COMPRESSION;
import static parquet.hadoop.ParquetOutputFormat.ENABLE_DICTIONARY;
import static parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static parquet.hadoop.metadata.CompressionCodecName.LZO;
import static parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

public class ParquetTester
{
    public static final DateTimeZone HIVE_STORAGE_TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");
    private static final boolean OPTIMIZED = true;
    private static final HiveClientConfig HIVE_CLIENT_CONFIG = createHiveClientConfig(false);
    private static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment(HIVE_CLIENT_CONFIG);
    private static final TestingConnectorSession SESSION = new TestingConnectorSession(new HiveSessionProperties(HIVE_CLIENT_CONFIG, new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
    private static final TestingConnectorSession SESSION_USE_NAME = new TestingConnectorSession(new HiveSessionProperties(createHiveClientConfig(true), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
    private static final List<String> TEST_COLUMN = singletonList("test");

    private Set<CompressionCodecName> compressions = ImmutableSet.of();

    private Set<WriterVersion> versions = ImmutableSet.of();

    private Set<TestingConnectorSession> sessions = ImmutableSet.of();

    public static ParquetTester quickParquetTester()
    {
        ParquetTester parquetTester = new ParquetTester();
        parquetTester.compressions = ImmutableSet.of(GZIP);
        parquetTester.versions = ImmutableSet.of(PARQUET_1_0);
        parquetTester.sessions = ImmutableSet.of(SESSION);
        return parquetTester;
    }

    public static ParquetTester fullParquetTester()
    {
        ParquetTester parquetTester = new ParquetTester();
        parquetTester.compressions = ImmutableSet.of(GZIP, UNCOMPRESSED, SNAPPY, LZO);
        parquetTester.versions = ImmutableSet.copyOf(WriterVersion.values());
        parquetTester.sessions = ImmutableSet.of(SESSION, SESSION_USE_NAME);
        return parquetTester;
    }

    void testWriteStruct()
            throws IOException
    {
        try (TempFile tempFile = new TempFile("test", "parquet")) {
            JobConf jobConf = new JobConf();
            jobConf.setEnum(COMPRESSION, UNCOMPRESSED);
            jobConf.setBoolean(ENABLE_DICTIONARY, false);
            // TODO not sure it's 2.0
            jobConf.setEnum(WRITER_VERSION, PARQUET_2_0);

            Type structType = RowType.from(asList(field("stringField", VARCHAR), field("longField", BIGINT)));

            ImmutableList<String> columnNames = ImmutableList.of("testStruct");
            ImmutableList<Type> columnTypes = ImmutableList.of(structType);

            ParquetWriter parquetWriter = new ParquetWriter(
                    new FileOutputStream(tempFile.getFile()),
                    columnNames,
                    columnTypes);

            RowPagesBuilder rowPagesBuilder = rowPagesBuilder(columnTypes);
            List<List<Object>> data = asList(null, asList("abc", null), ImmutableList.of("ad", 3L), null);
            for (int i = 0; i < data.size(); ++i) {
                rowPagesBuilder.row(data.get(i));
            }
            rowPagesBuilder.pageBreak();
            List<Page> pages = rowPagesBuilder.build();
            for (Page page : pages) {
                parquetWriter.write(page);
            }
            parquetWriter.close();

            assertFileContents(SESSION, tempFile.getFile(), getIterators(new Iterable[] {data}), columnNames, columnTypes);
        }
    }

    void testWriteArray()
            throws IOException
    {
        try (TempFile tempFile = new TempFile("test", "parquet")) {
            JobConf jobConf = new JobConf();
            jobConf.setEnum(COMPRESSION, UNCOMPRESSED);
            jobConf.setBoolean(ENABLE_DICTIONARY, false);
            // TODO not sure it's 2.0
            jobConf.setEnum(WRITER_VERSION, PARQUET_2_0);

            Type type = new ArrayType(createUnboundedVarcharType());
            ImmutableList<String> columnNames = ImmutableList.of("array");
            ImmutableList<Type> columnTypes = ImmutableList.of(type);

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
            ImmutableList.Builder<Page> pages = ImmutableList.builder();

            ImmutableList.Builder<List<Object>> listBuilder = ImmutableList.builder();

            Random random = new Random(1234);
            for (int count = 0; count < 10; count++) {
                pageBuilder.declarePosition();

                BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                BlockBuilder mapBuilder = builder.beginBlockEntry();
                ImmutableList.Builder<Object> expectValueBuilder = ImmutableList.builder();
                int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                for (int entryId = 0; entryId < entries; entryId++) {
                    String value = "key" + random.nextInt(10_000_000);
                    createUnboundedVarcharType().writeSlice(mapBuilder, utf8Slice(value));
                    expectValueBuilder.add(value);
                }
                builder.closeEntry();
                listBuilder.add(expectValueBuilder.build());
                Preconditions.checkArgument(!pageBuilder.isFull());
            }

            pages.add(pageBuilder.build());
            pageBuilder.reset();

            ParquetWriter parquetWriter = new ParquetWriter(
                    new FileOutputStream(tempFile.getFile()),
                    columnNames,
                    columnTypes);

            for (Page page : pages.build()) {
                parquetWriter.write(page);
            }
            parquetWriter.close();

            assertFileContents(SESSION, tempFile.getFile(), getIterators(new Iterable[] {listBuilder.build()}), columnNames, columnTypes);
        }
    }

    private static final int MIN_ENTRIES = 1;
    private static final int MAX_ENTRIES = 5;

    private static int nextRandomBetween(Random random, int min, int max)
    {
        return min + random.nextInt(max - min);
    }

    public void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<?> writeValues, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, writeValues, parameterType);
    }

    public <W, R> void testRoundTrip(PrimitiveObjectInspector columnObjectInspector, Iterable<W> writeValues, Function<W, R> readTransform, Type parameterType)
            throws Exception
    {
        testRoundTrip(columnObjectInspector, writeValues, transform(writeValues, readTransform), parameterType);
    }

    public void testSingleLevelArraySchemaRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        ArrayList<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(objectInspector.getTypeName());
        MessageType schema = SingleLevelArraySchemaConverter.convert(TEST_COLUMN, typeInfos);
        testSingleLevelArrayRoundTrip(objectInspector, writeValues, readValues, type, Optional.of(schema));
        if (objectInspector.getTypeName().contains("map<")) {
            schema = SingleLevelArrayMapKeyValuesSchemaConverter.convert(TEST_COLUMN, typeInfos);
            testSingleLevelArrayRoundTrip(objectInspector, writeValues, readValues, type, Optional.of(schema));
        }
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type)
            throws Exception
    {
        // just the values
        testRoundTripType(singletonList(objectInspector), new Iterable<?>[] {writeValues},
                new Iterable<?>[] {readValues}, TEST_COLUMN, singletonList(type), Optional.empty(), false);

        // all nulls
        assertRoundTrip(singletonList(objectInspector), new Iterable<?>[] {transform(writeValues, constant(null))},
                new Iterable<?>[] {transform(writeValues, constant(null))}, TEST_COLUMN, singletonList(type), Optional.empty());
        if (objectInspector.getTypeName().contains("map<")) {
            ArrayList<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(objectInspector.getTypeName());
            MessageType schema = MapKeyValuesSchemaConverter.convert(TEST_COLUMN, typeInfos);
            // just the values
            testRoundTripType(singletonList(objectInspector), new Iterable<?>[] {writeValues}, new Iterable<?>[] {
                    readValues}, TEST_COLUMN, singletonList(type), Optional.of(schema), false);

            // all nulls
            assertRoundTrip(singletonList(objectInspector), new Iterable<?>[] {transform(writeValues, constant(null))},
                    new Iterable<?>[] {transform(writeValues, constant(null))}, TEST_COLUMN, singletonList(type), Optional.of(schema));
        }
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type, Optional<MessageType> parquetSchema)
            throws Exception
    {
        testRoundTrip(singletonList(objectInspector), new Iterable<?>[] {writeValues}, new Iterable<?>[] {readValues}, TEST_COLUMN, singletonList(type), parquetSchema, false);
    }

    public void testSingleLevelArrayRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, Type type, Optional<MessageType> parquetSchema)
            throws Exception
    {
        testRoundTrip(singletonList(objectInspector), new Iterable<?>[] {writeValues}, new Iterable<?>[] {readValues}, TEST_COLUMN, singletonList(type), parquetSchema, true);
    }

    public void testRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, String columnName, Type type, Optional<MessageType> parquetSchema)
            throws Exception
    {
        testRoundTrip(singletonList(objectInspector), new Iterable<?>[] {writeValues}, new Iterable<?>[] {readValues}, singletonList(columnName), singletonList(type), parquetSchema, false);
    }

    public void testSingleLevelArrayRoundTrip(ObjectInspector objectInspector, Iterable<?> writeValues, Iterable<?> readValues, String columnName, Type type, Optional<MessageType> parquetSchema)
            throws Exception
    {
        testRoundTrip(singletonList(objectInspector), new Iterable<?>[] {writeValues}, new Iterable<?>[] {readValues}, singletonList(columnName), singletonList(type), parquetSchema, true);
    }

    public void testRoundTrip(List<ObjectInspector> objectInspectors, Iterable<?>[] writeValues, Iterable<?>[] readValues, List<String> columnNames, List<Type> columnTypes, Optional<MessageType> parquetSchema, boolean singleLevelArray)
            throws Exception
    {
        // just the values
        testRoundTripType(objectInspectors, writeValues, readValues, columnNames, columnTypes, parquetSchema, singleLevelArray);

        // all nulls
        //assertRoundTrip(objectInspectors, transformToNulls(writeValues), transformToNulls(readValues), columnNames, columnTypes, parquetSchema, singleLevelArray);
    }

    private void testRoundTripType(List<ObjectInspector> objectInspectors, Iterable<?>[] writeValues, Iterable<?>[] readValues,
            List<String> columnNames, List<Type> columnTypes, Optional<MessageType> parquetSchema, boolean singleLevelArray)
            throws Exception
    {
        // forward order
        assertRoundTrip(objectInspectors, writeValues, readValues, columnNames, columnTypes, parquetSchema, singleLevelArray);

        // reverse order
        assertRoundTrip(objectInspectors, reverse(writeValues), reverse(readValues), columnNames, columnTypes, parquetSchema, singleLevelArray);

        // forward order with nulls
        assertRoundTrip(objectInspectors, insertNullEvery(5, writeValues), insertNullEvery(5, readValues), columnNames, columnTypes, parquetSchema, singleLevelArray);

        // reverse order with nulls
        assertRoundTrip(objectInspectors, insertNullEvery(5, reverse(writeValues)), insertNullEvery(5, reverse(readValues)), columnNames, columnTypes, parquetSchema, singleLevelArray);
    }

    void assertRoundTrip(List<ObjectInspector> objectInspectors,
            Iterable<?>[] writeValues,
            Iterable<?>[] readValues,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<MessageType> parquetSchema)
            throws Exception
    {
        assertRoundTrip(objectInspectors, writeValues, readValues, columnNames, columnTypes, parquetSchema, false);
    }

    void assertRoundTrip(List<ObjectInspector> objectInspectors,
            Iterable<?>[] writeValues,
            Iterable<?>[] readValues,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<MessageType> parquetSchema,
            boolean singleLevelArray)
            throws Exception
    {
        assertRoundTrip(objectInspectors, writeValues, readValues, columnNames, columnTypes, Optional.empty(), parquetSchema, singleLevelArray);
    }

    void assertRoundTrip(List<ObjectInspector> objectInspectors,
            Iterable<?>[] writeValues,
            Iterable<?>[] readValues,
            List<String> columnNames,
            List<Type> columnTypes,
            Optional<List<String>> rootColumns,
            Optional<MessageType> parquetSchema,
            boolean singleLevelArray)
            throws Exception
    {
        for (WriterVersion version : versions) {
            for (CompressionCodecName compressionCodecName : compressions) {
                for (ConnectorSession session : sessions) {
                    try (TempFile tempFile = new TempFile("test", "parquet")) {
                        JobConf jobConf = new JobConf();
                        jobConf.setEnum(COMPRESSION, compressionCodecName);
                        jobConf.setBoolean(ENABLE_DICTIONARY, true);
                        jobConf.setEnum(WRITER_VERSION, version);
                        List<String> writeColumns = rootColumns.isPresent() ? rootColumns.get() : columnNames;
                        writeParquetColumnPresto(tempFile.getFile(), columnTypes, columnNames, getIterators(writeValues));
//                        writeParquetColumn(
//                                jobConf,
//                                tempFile.getFile(),
//                                compressionCodecName,
//                                createTableProperties(writeColumns, objectInspectors),
//                                getStandardStructObjectInspector(writeColumns, objectInspectors),
//                                getIterators(writeValues),
//                                parquetSchema,
//                                singleLevelArray);
                        assertFileContents(
                                session,
                                tempFile.getFile(),
                                getIterators(readValues),
                                columnNames,
                                columnTypes);
                    }
                }
            }
        }
    }

    private static void assertFileContents(
            ConnectorSession session,
            File dataFile,
            Iterator<?>[] expectedValues,
            List<String> columnNames,
            List<Type> columnTypes)
            throws IOException
    {
        try (ConnectorPageSource pageSource = getFileFormat().createFileFormatReader(
                session,
                HDFS_ENVIRONMENT,
                dataFile,
                columnNames,
                columnTypes)) {
            if (pageSource instanceof RecordPageSource) {
                assertRecordCursor(columnTypes, expectedValues, ((RecordPageSource) pageSource).getCursor());
            }
            else {
                assertPageSource(columnTypes, expectedValues, pageSource);
            }
            assertFalse(stream(expectedValues).allMatch(Iterator::hasNext));
        }
    }

    private static void assertPageSource(List<Type> types, Iterator<?>[] valuesByField, ConnectorPageSource pageSource)
    {
        Page page;
        while ((page = pageSource.getNextPage()) != null) {
            for (int field = 0; field < page.getChannelCount(); field++) {
                Block block = page.getBlock(field);
                for (int i = 0; i < block.getPositionCount(); i++) {
                    assertTrue(valuesByField[field].hasNext());
                    Object expected = valuesByField[field].next();
                    Object actual = decodeObject(types.get(field), block, i);
                    assertEquals(actual, expected);
                }
            }
        }
    }

    private static void assertRecordCursor(List<Type> types, Iterator<?>[] valuesByField, RecordCursor cursor)
    {
        while (cursor.advanceNextPosition()) {
            for (int field = 0; field < types.size(); field++) {
                assertTrue(valuesByField[field].hasNext());
                Object expected = valuesByField[field].next();
                Object actual = getActualCursorValue(cursor, types.get(field), field);
                assertEquals(actual, expected);
            }
        }
    }

    private static Object getActualCursorValue(RecordCursor cursor, Type type, int field)
    {
        Object fieldFromCursor = getFieldFromCursor(cursor, type, field);
        if (fieldFromCursor == null) {
            return null;
        }
        if (isStructuralType(type)) {
            Block block = (Block) fieldFromCursor;
            if (isArrayType(type)) {
                Type elementType = ((ArrayType) type).getElementType();
                return toArrayValue(block, elementType);
            }
            else if (isMapType(type)) {
                MapType mapType = (MapType) type;
                return toMapValue(block, mapType.getKeyType(), mapType.getValueType());
            }
            else if (isRowType(type)) {
                return toRowValue(block, type.getTypeParameters());
            }
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return new SqlDecimal((BigInteger) fieldFromCursor, decimalType.getPrecision(), decimalType.getScale());
        }
        if (isVarcharType(type)) {
            return new String(((Slice) fieldFromCursor).getBytes());
        }
        if (VARBINARY.equals(type)) {
            return new SqlVarbinary(((Slice) fieldFromCursor).getBytes());
        }
        if (DATE.equals(type)) {
            return new SqlDate(((Long) fieldFromCursor).intValue());
        }
        if (TIMESTAMP.equals(type)) {
            return new SqlTimestamp((long) fieldFromCursor, UTC_KEY);
        }
        return fieldFromCursor;
    }

    private static Map toMapValue(Block mapBlock, Type keyType, Type valueType)
    {
        Map<Object, Object> map = new HashMap<>(mapBlock.getPositionCount() * 2);
        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            map.put(keyType.getObjectValue(SESSION, mapBlock, i), valueType.getObjectValue(SESSION, mapBlock, i + 1));
        }
        return Collections.unmodifiableMap(map);
    }

    private static List toArrayValue(Block arrayBlock, Type elementType)
    {
        List<Object> values = new ArrayList<>();
        for (int position = 0; position < arrayBlock.getPositionCount(); position++) {
            values.add(elementType.getObjectValue(SESSION, arrayBlock, position));
        }
        return Collections.unmodifiableList(values);
    }

    private static List toRowValue(Block rowBlock, List<Type> fieldTypes)
    {
        List<Object> values = new ArrayList<>(rowBlock.getPositionCount());
        for (int i = 0; i < rowBlock.getPositionCount(); i++) {
            values.add(fieldTypes.get(i).getObjectValue(SESSION, rowBlock, i));
        }
        return Collections.unmodifiableList(values);
    }

    private static HiveClientConfig createHiveClientConfig(boolean useParquetColumnNames)
    {
        HiveClientConfig config = new HiveClientConfig();
        config.setHiveStorageFormat(HiveStorageFormat.PARQUET)
                .setUseParquetColumnNames(useParquetColumnNames);
        return config;
    }

    private static FileFormat getFileFormat()
    {
        return OPTIMIZED ? FileFormat.PRESTO_PARQUET : FileFormat.HIVE_PARQUET;
    }

    private static DataSize writeParquetColumn(JobConf jobConf,
            File outputFile,
            CompressionCodecName compressionCodecName,
            Properties tableProperties,
            SettableStructObjectInspector objectInspector,
            Iterator<?>[] valuesByField,
            Optional<MessageType> parquetSchema,
            boolean singleLevelArray)
            throws Exception
    {
        RecordWriter recordWriter = new TestMapredParquetOutputFormat(parquetSchema, singleLevelArray)
                .getHiveRecordWriter(
                        jobConf,
                        new Path(outputFile.toURI()),
                        Text.class,
                        compressionCodecName != UNCOMPRESSED,
                        tableProperties,
                        () -> {});
        Object row = objectInspector.create();
        List<StructField> fields = ImmutableList.copyOf(objectInspector.getAllStructFieldRefs());
        while (stream(valuesByField).allMatch(Iterator::hasNext)) {
            for (int field = 0; field < fields.size(); field++) {
                Object value = valuesByField[field].next();
                objectInspector.setStructFieldData(row, fields.get(field), value);
            }
            ParquetHiveSerDe serde = new ParquetHiveSerDe();
            serde.initialize(jobConf, tableProperties, null);
            Writable record = serde.serialize(row, objectInspector);
            recordWriter.write(record);
        }
        recordWriter.close(false);
        return succinctBytes(outputFile.length());
    }

    private static Properties createTableProperties(List<String> columnNames, List<ObjectInspector> objectInspectors)
    {
        Properties orderTableProperties = new Properties();
        orderTableProperties.setProperty("columns", Joiner.on(',').join(columnNames));
        orderTableProperties.setProperty("columns.types", Joiner.on(',').join(transform(objectInspectors, ObjectInspector::getTypeName)));
        return orderTableProperties;
    }

    static class TempFile
            implements Closeable
    {
        private final File file;

        public TempFile(String prefix, String suffix)
        {
            try {
                file = File.createTempFile(prefix, suffix);
                file.delete();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public File getFile()
        {
            return file;
        }

        @Override
        public void close()
        {
            file.delete();
        }
    }

    private Iterator<?>[] getIterators(Iterable<?>[] values)
    {
        return stream(values).map(Iterable::iterator).toArray(size -> new Iterator<?>[size]);
    }

    private Iterable<?>[] transformToNulls(Iterable<?>[] values)
    {
        return stream(values)
                .map(v -> transform(v, constant(null)))
                .toArray(size -> new Iterable<?>[size]);
    }

    private static Iterable<?>[] reverse(Iterable<?>[] iterables)
    {
        return stream(iterables)
                .map(ImmutableList::copyOf)
                .map(Lists::reverse)
                .toArray(size -> new Iterable<?>[size]);
    }

    static Iterable<?>[] insertNullEvery(int n, Iterable<?>[] iterables)
    {
        return stream(iterables)
                .map(itr -> insertNullEvery(n, itr))
                .toArray(size -> new Iterable<?>[size]);
    }

    static <T> Iterable<T> insertNullEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                position++;
                if (position > n) {
                    position = 0;
                    return null;
                }

                if (!delegate.hasNext()) {
                    return endOfData();
                }

                return delegate.next();
            }
        };
    }

    private static Object decodeObject(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return type.getObjectValue(SESSION, block, position);
    }

    private static void writeParquetColumnPresto(File outputFile, List<Type> types, List<String> columnNames, Iterator<?>[] values)
            throws Exception
    {
        Preconditions.checkArgument(types.size() == 1);
        Preconditions.checkArgument(types.size() == columnNames.size() && types.size() == values.length);
        ParquetWriter writer = new ParquetWriter(
                new FileOutputStream(outputFile),
                columnNames,
                types);

        int i = 0;
        Type type = types.get(i);
        Iterator<?> iterator = values[i];
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1024);
        while (iterator.hasNext()) {
            Object value = iterator.next();
            writeValue(type, blockBuilder, value);
        }

        writer.write(new Page(blockBuilder.build()));
        writer.close();
    }

    private static void writeValue(Type type, BlockBuilder blockBuilder, Object value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            if (BOOLEAN.equals(type)) {
                type.writeBoolean(blockBuilder, (Boolean) value);
            }
            else if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
                type.writeLong(blockBuilder, ((Number) value).longValue());
            }
            else if (Decimals.isShortDecimal(type)) {
                type.writeLong(blockBuilder, ((SqlDecimal) value).toBigDecimal().unscaledValue().longValue());
            }
            else if (Decimals.isLongDecimal(type)) {
                type.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(((SqlDecimal) value).toBigDecimal().unscaledValue()));
            }
            else if (DOUBLE.equals(type)) {
                type.writeDouble(blockBuilder, ((Number) value).doubleValue());
            }
            else if (REAL.equals(type)) {
                float floatValue = ((Number) value).floatValue();
                type.writeLong(blockBuilder, Float.floatToIntBits(floatValue));
            }
            else if (type instanceof VarcharType) {
                Slice slice = truncateToLength(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (type instanceof CharType) {
                Slice slice = truncateToLengthAndTrimSpaces(utf8Slice((String) value), type);
                type.writeSlice(blockBuilder, slice);
            }
            else if (VARBINARY.equals(type)) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) value));
            }
            else if (DATE.equals(type)) {
                long days = ((SqlDate) value).getDays();
                type.writeLong(blockBuilder, days);
            }
            else if (TIMESTAMP.equals(type)) {
                long millis = ((SqlTimestamp) value).getMillisUtc();
                type.writeLong(blockBuilder, millis);
            }
            else {
                String baseType = type.getTypeSignature().getBase();
                if (StandardTypes.ARRAY.equals(baseType)) {
                    List<?> array = (List<?>) value;
                    Type elementType = type.getTypeParameters().get(0);
                    BlockBuilder arrayBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Object elementValue : array) {
                        writeValue(elementType, arrayBlockBuilder, elementValue);
                    }
                    blockBuilder.closeEntry();
                }
                else if (StandardTypes.MAP.equals(baseType)) {
                    Map<?, ?> map = (Map<?, ?>) value;
                    Type keyType = type.getTypeParameters().get(0);
                    Type valueType = type.getTypeParameters().get(1);
                    BlockBuilder mapBlockBuilder = blockBuilder.beginBlockEntry();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        writeValue(keyType, mapBlockBuilder, entry.getKey());
                        writeValue(valueType, mapBlockBuilder, entry.getValue());
                    }
                    blockBuilder.closeEntry();
                }
                else if (StandardTypes.ROW.equals(baseType)) {
                    List<?> array = (List<?>) value;
                    List<Type> fieldTypes = type.getTypeParameters();
                    BlockBuilder rowBlockBuilder = blockBuilder.beginBlockEntry();
                    for (int fieldId = 0; fieldId < fieldTypes.size(); fieldId++) {
                        Type fieldType = fieldTypes.get(fieldId);
                        writeValue(fieldType, rowBlockBuilder, array.get(fieldId));
                    }
                    blockBuilder.closeEntry();
                }
                else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        }
    }
}
