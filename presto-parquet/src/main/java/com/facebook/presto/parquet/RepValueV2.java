package com.facebook.presto.parquet;

import com.facebook.presto.parquet.RepetitionValueIterator.RepetitionValue;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.List;

import static com.facebook.presto.parquet.RepetitionValueIterator.nonNullValue;
import static com.facebook.presto.parquet.RepetitionValueIterator.nullValue;

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

    static RepValueV2 getIterator(Block block)
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

    static RepValueV2 getIterator(ColumnarArray columnarArray, int maxRepetionValue)
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
                    iterator = new FixedValueIterator(columnarArray.getLength(position), getValue(), maxRepetionValue);
                    Preconditions.checkArgument(iterator.hasNext(), "Not support empty array yet");
                    return iterator.next();
                }
            }
        };
    }

    private static class FixedValueIterator
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

    public static class RepRepIterator
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
