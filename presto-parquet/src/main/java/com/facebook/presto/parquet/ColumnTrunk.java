package com.facebook.presto.parquet;

import com.facebook.presto.spi.block.Block;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

public class ColumnTrunk
{
    private final Block block;
    private final DefinitionValueIterator defIterator;
    private final RepetitionValueIterator repIterator;

    ColumnTrunk(Block block)
    {
        this.block = block;
        this.defIterator = null;
        this.repIterator = null;
    }

    ColumnTrunk(Block block, DefinitionValueIterator defIterator, RepetitionValueIterator repIterator)
    {
        this.block = block;
        this.defIterator = defIterator;
        this.repIterator = repIterator;
    }

    public Block getBlock()
    {
        return block;
    }

    public Iterator<Optional<Integer>> getDefIterator()
    {
        return defIterator == null ? Collections.emptyIterator() : defIterator.iterator();
    }

    public Iterator<RepetitionValueIterator.RepetitionValue> getRepIterator()
    {
        return repIterator == null ? Collections.emptyIterator() : repIterator.iterator();
    }
}
