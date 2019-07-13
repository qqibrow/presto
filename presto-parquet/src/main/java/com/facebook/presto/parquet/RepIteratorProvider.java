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

import com.facebook.presto.parquet.RepetitionValueIterator.RepetitionValue;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.List;

import static com.facebook.presto.parquet.RepetitionValueIterator.nonNullValue;
import static com.facebook.presto.parquet.RepetitionValueIterator.nullValue;

public interface RepIteratorProvider
{
    RepValueV2 getIterator();

    public abstract class RepValueV2
            extends AbstractIterator<RepetitionValue>
    {
        private int value;

        abstract boolean end();

        void setValue(int value)
        {
            this.value = value;
        }

        int getValue()
        {
            return value;
        }
    }

    class BlockRepIterator
            implements RepIteratorProvider
    {
        private final Block block;

        BlockRepIterator(Block block)
        {
            this.block = block;
        }

        @Override
        public RepValueV2 getIterator()
        {
            return new RepValueV2()
            {
                private int position = -1;

                @Override
                boolean end()
                {
                    return true;
                }

                @Override
                protected RepetitionValue computeNext()
                {
                    position++;
                    if (position == block.getPositionCount()) {
                        return endOfData();
                    }
                    if (block.isNull(position)) {
                        return nullValue(getValue());
                    }
                    else {
                        return nonNullValue(getValue());
                    }
                }
            };
        }
    }

    class ArrayRepIterator
            implements RepIteratorProvider
    {
        private final ColumnarArray columnarArray;
        private final int maxRepetionValue;

        ArrayRepIterator(ColumnarArray columnarArray, int maxRepetionValue)
        {
            this.columnarArray = columnarArray;
            this.maxRepetionValue = maxRepetionValue;
        }

        @Override
        public RepValueV2 getIterator()
        {
            return new RepValueV2()
            {
                private int position = -1;
                private FixedValueIterator iterator;

                @Override
                boolean end()
                {
                    return iterator == null || !iterator.hasNext();
                }

                @Override
                protected RepetitionValue computeNext()
                {
                    if (iterator != null && iterator.hasNext()) {
                        return iterator.next();
                    }
                    position++;
                    if (position == columnarArray.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarArray.isNull(position)) {
                        return nullValue(getValue());
                    }
                    else {
                        int arrayLength = columnarArray.getLength(position);
                        if (arrayLength == 0) {
                            return nullValue(getValue());
                        }
                        iterator = new FixedValueIterator(arrayLength, getValue(), maxRepetionValue);
                        return iterator.next();
                    }
                }
            };
        }
    }

    class FixedValueIterator
            extends AbstractIterator<RepetitionValue>
    {
        private final int length;
        private final int parentValue;
        private final int currentValue;
        private int position = -1;

        FixedValueIterator(int length, int parentValue, int currentValue)
        {
            this.length = length;
            this.parentValue = parentValue;
            this.currentValue = currentValue;
        }

        @Override
        protected RepetitionValue computeNext()
        {
            position++;
            if (position < length) {
                if (position == 0) {
                    return nonNullValue(parentValue);
                }
                else {
                    return nonNullValue(currentValue);
                }
            }
            return endOfData();
        }
    }

    class RepRepIterator
            extends AbstractIterator<Integer>
    {
        private final List<RepValueV2> repValueV2List;
        private int iteratorIndex;

        RepRepIterator(List<RepValueV2> repValueV2List)
        {
            this.repValueV2List = repValueV2List;
        }

        @Override
        protected Integer computeNext()
        {
            RepValueV2 current = repValueV2List.get(iteratorIndex);
            while (iteratorIndex > 0 && current.end()) {
                current = repValueV2List.get(--iteratorIndex);
            }

            while (current.hasNext()) {
                RepetitionValue next = current.next();
                if (next.isNull() || iteratorIndex == repValueV2List.size() - 1) {
                    return next.getValue();
                }
                else {
                    int lastValue = next.getValue();
                    current = repValueV2List.get(++iteratorIndex);
                    current.setValue(lastValue);
                }
            }
            Preconditions.checkArgument(repValueV2List.stream().noneMatch(AbstractIterator::hasNext));
            return endOfData();
        }
    }
}
