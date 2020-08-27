/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.values.storable;

import org.neo4j.blob.BlobId;
import org.neo4j.values.AnyValue;
import org.neo4j.blob.Blob;
import org.neo4j.values.ValueMapper;

public class BlobArray extends NonPrimitiveArray<Blob> {
    private Blob[] _blobs;
    private BlobArrayProvider _provider;
    BlobId _groupId;

    public BlobArray(BlobId id, BlobArrayProvider provider) {
        this._groupId = id;
        _provider = provider;
    }

    private Blob[] blobs() {
        if (_blobs == null) {
            _blobs = _provider.get();
        }

        return _blobs;
    }

    public BlobId groupId() {
        return this._groupId;
    }

    @Override
    public Blob[] value() {
        return this.blobs();
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return (T) blobs();
    }

    @Override
    public String getTypeName() {
        return "BlobArray";
    }

    @Override
    public AnyValue value(int offset) {
        return new BlobValue(blobs()[offset]);
    }

    @Override
    public boolean equals(Value other) {
        return this == other;
    }

    @Override
    int unsafeCompareTo(Value other) {
        return this.equals(other) ? 0 : -1;
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueGroup valueGroup() {
        return ValueGroup.NO_VALUE;
    }
}
