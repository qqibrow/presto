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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ParquetWriter
    implements Closeable
{
    private final List<ColumnWriter> columnWriters;
    private final MetadataWriter metadataWriter;
    private final OutputStream outputStream;
    private final List<Type> types;

    public ParquetWriter(OutputStream outputStream, List<String> columnNames, List<Type> types)
    {
        this.outputStream = requireNonNull(outputStream, "outputstream is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.columnWriters = ImmutableList.of(new LongColumnWriter());
        this.metadataWriter = new MetadataWriter();
    }

    public void write(com.facebook.presto.spi.Page page)
        throws IOException
    {

    }

    @Override
    public void close()
            throws IOException
    {

    }
}
