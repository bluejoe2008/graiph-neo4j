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
package org.neo4j.bolt.blob

import org.neo4j.blob._
import org.neo4j.blob.impl.{BlobFactory, BlobIdFactory}
import org.neo4j.blob.util.ReflectUtils._
import org.neo4j.blob.util._
import org.neo4j.kernel.impl.KernelTransactionEventHub
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BlobValue

/**
  * Created by bluejoe on 2019/4/3.
  */
object BoltServerBlobIO {
  val INIT_CHUNK_SIZE = 1024 * 100; //100k
  val useInlineAlways = false;

  def packBlob(blob: Blob, out: org.neo4j.bolt.v1.packstream.PackOutput): BlobEntry = {
    val inline = useInlineAlways || (blob.length <= BlobIO.MAX_INLINE_BLOB_BYTES);
    //write marker
    out.writeByte(if (inline) {
      BlobIO.BOLT_VALUE_TYPE_BLOB_INLINE
    }
    else {
      BlobIO.BOLT_VALUE_TYPE_BLOB_REMOTE
    });

    //write blob entry
    val entry = blob match {
      case e: BlobEntry => e
      case _ => BlobFactory.makeEntry(BlobIdFactory.EMPTY, blob)
    }

    BlobIO._pack(entry).foreach(out.writeLong(_));
    //write inline
    if (inline) {
      val bs = blob.toBytes();
      out.writeBytes(bs, 0, bs.length);
    }
    else {
      //cache blob here for later use
      val handle = KernelTransactionEventHub.cacheBlob(blob);

      //pack blob id
      val bytesHandle = handle.getBytes("utf-8")
      out.writeInt(bytesHandle.length)
      out.writeBytes(bytesHandle, 0, bytesHandle.length)
    }

    entry
  }

  def unpackBlob(unpacker: org.neo4j.bolt.v1.packstream.PackStream.Unpacker): AnyValue = {
    val in = unpacker._get("in").asInstanceOf[org.neo4j.bolt.v1.packstream.PackInput];
    val byte = in.peekByte();

    byte match {
      case BlobIO.BOLT_VALUE_TYPE_BLOB_INLINE =>
        in.readByte();

        val values = for (i <- 0 to 3) yield in.readLong();
        val entry = BlobIO.unpack(values.toArray);

        //read inline
        val length = entry.length;
        val bs = new Array[Byte](length.toInt);
        in.readBytes(bs, 0, length.toInt);
        new BlobValue((new InlineBlob(bs, length, entry.mimeType)));

      case _ =>
        null;
    }
  }
}