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
package com.facebook.presto.spi;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

// not support array now
public class NestedColumn
{
    private Type type;
    private List<String> names;

    @JsonCreator
    public NestedColumn(@JsonProperty("names") List<String> names, @JsonProperty("type") Type type)
    {
        this.names = requireNonNull(names);
        this.type = requireNonNull(type);
        // TODO check names.size() >= 2
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public List<String> getNames()
    {
        return names;
    }

    public String getBase()
    {
        return names.get(0);
    }

    public String getName()
    {
        return names.stream().collect(Collectors.joining("."));
    }
}
