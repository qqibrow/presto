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
