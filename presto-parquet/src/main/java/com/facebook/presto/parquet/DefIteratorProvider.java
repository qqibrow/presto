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
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.List;
import java.util.Optional;

public interface DefIteratorProvider
{
    abstract class DefValueV2
            extends AbstractIterator<Optional<Integer>>
    {
        abstract boolean end();
    }

    DefValueV2 getIterator();

    class BlockDefValueProvider
            implements DefIteratorProvider
    {
        private final Block block;
        private final int maxDefinitionLevel;

        BlockDefValueProvider(Block block, int maxDefinitionLevel)
        {
            this.block = block;
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefValueV2 getIterator()
        {
            return new DefValueV2()
            {
                private int position = -1;

                @Override
                boolean end()
                {
                    return true;
                }

                @Override
                protected Optional<Integer> computeNext()
                {
                    position++;
                    if (position == block.getPositionCount()) {
                        return endOfData();
                    }
                    if (block.isNull(position)) {
                        return Optional.of(maxDefinitionLevel - 1);
                    }
                    else {
                        return Optional.of(maxDefinitionLevel);
                    }
                }
            };
        }
    }

    class ColumnRowIteratorProvider
            implements DefIteratorProvider
    {
        private final ColumnarRow columnarRow;
        private final int maxDefinitionLevel;

        ColumnRowIteratorProvider(ColumnarRow columnarRow, int maxDefinitionLevel)
        {
            this.columnarRow = columnarRow;
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefValueV2 getIterator()
        {
            return new DefValueV2()
            {
                private int position = -1;

                @Override
                boolean end()
                {
                    return true;
                }

                @Override
                protected Optional<Integer> computeNext()
                {
                    position++;
                    if (position == columnarRow.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarRow.isNull(position)) {
                        return Optional.of(maxDefinitionLevel - 1);
                    }
                    else {
                        return Optional.empty();
                    }
                }
            };
        }
    }

    class ColumnMapIteratorProvider
            implements DefIteratorProvider
    {
        private final ColumnarMap columnarMap;
        private final int maxDefinitionLevel;

        ColumnMapIteratorProvider(ColumnarMap columnarMap, int maxDefinitionLevel)
        {
            this.columnarMap = columnarMap;
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefValueV2 getIterator()
        {
            return new DefValueV2()
            {
                private int position = -1;

                @Override
                boolean end()
                {
                    return true;
                }

                @Override
                protected Optional<Integer> computeNext()
                {
                    position++;
                    if (position == columnarMap.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarMap.isNull(position)) {
                        return Optional.of(maxDefinitionLevel - 1);
                    }
                    else {
                        return Optional.empty();
                    }
                }
            };
        }
    }

    class ColumnArrayIteratorProvider
            implements DefIteratorProvider
    {
        private final ColumnarArray columnarArray;
        private final int maxDefinitionLevel;

        ColumnArrayIteratorProvider(ColumnarArray columnarArray, int maxDefinitionLevel)
        {
            this.columnarArray = columnarArray;
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefValueV2 getIterator()
        {
            return new DefValueV2()
            {
                private int position = -1;
                private FixedValueIterator iterator;

                @Override
                boolean end()
                {
                    return iterator == null || !iterator.hasNext();
                }

                @Override
                protected Optional<Integer> computeNext()
                {
                    if (iterator != null && iterator.hasNext()) {
                        return iterator.next();
                    }
                    position++;
                    if (position == columnarArray.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarArray.isNull(position)) {
                        return Optional.of(maxDefinitionLevel - 2);
                    }
                    else {
                        int arrayLength = columnarArray.getLength(position);
                        if (arrayLength == 0) {
                            return Optional.of(maxDefinitionLevel - 1);
                        }
                        iterator = new FixedValueIterator(arrayLength);
                        Preconditions.checkArgument(iterator.hasNext(), "Not support empty array yet");
                        return iterator.next();
                    }
                }
            };
        }
    }

    class FixedValueIterator
            extends AbstractIterator<Optional<Integer>>
    {
        private final int length;
        private int position = -1;

        FixedValueIterator(int length)
        {
            this.length = length;
        }

        @Override
        protected Optional<Integer> computeNext()
        {
            position++;
            if (position < length) {
                return Optional.empty();
            }
            return endOfData();
        }
    }

    class DefDefIterator
            extends AbstractIterator<Integer>
    {
        private final List<DefValueV2> defValueV2List;
        private int iteratorIndex;

        DefDefIterator(List<DefValueV2> defValueV2List)
        {
            this.defValueV2List = defValueV2List;
        }

        @Override
        protected Integer computeNext()
        {
            DefValueV2 current = defValueV2List.get(iteratorIndex);
            while (iteratorIndex > 0 && current.end()) {
                iteratorIndex--;
                current = defValueV2List.get(iteratorIndex);
            }

            while (current.hasNext()) {
                Optional<Integer> next = current.next();
                if (next.isPresent()) {
                    return next.get();
                }
                else {
                    iteratorIndex++;
                    current = defValueV2List.get(iteratorIndex);
                }
            }
            Preconditions.checkArgument(defValueV2List.stream().noneMatch(AbstractIterator::hasNext));
            return endOfData();
        }
    }
}
