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
package org.neo4j.kernel.impl.blob

import java.io.InputStream
import java.nio.ByteBuffer
import java.util.ServiceLoader

import org.neo4j.blob._
import org.neo4j.blob.impl.BlobFactory
import org.neo4j.blob.util._
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyBlock, PropertyRecord}
import org.neo4j.values.storable.{BlobArray, BlobArrayProvider, BlobValue, Values}

/**
  * Created by bluejoe on 2019/3/29.
  */
trait StoreBlobIOService {
  def saveBlobArray(buf: ByteBuffer, blobs: Array[Blob], ic: ContextMap)

  def saveBlob(ic: ContextMap, blob: Blob, keyId: Int, block: PropertyBlock)

  def deleteBlobArrayProperty(ic: ContextMap, blobs: BlobArray): Unit

  def deleteBlobProperty(ic: ContextMap, block: PropertyBlock): Unit

  def readBlobArray(ic: ContextMap, dataBuffer: ByteBuffer, arrayLength: Int): BlobArray

  def readBlobValue(ic: ContextMap, values: Array[Long]): BlobValue
}

class DefaultStoreBlobIOService extends StoreBlobIOService {
  override def saveBlobArray(buf: ByteBuffer, blobs: Array[Blob], ic: ContextMap) = {
    val bid = ic.get[BlobStorage].saveGroup(blobs);
    val bytes = bid.asByteArray()
    buf.put(bytes)
  }

  /*
  def saveAndEncodeBlobAsByteArray(ic: ContextMap, blob: Blob): Array[Byte] = {
    val bid = ic.get[BlobStorage].save(blob);
    BlobIO.pack(BlobFactory.makeEntry(bid, blob));
  }
   */

  override def saveBlob(ic: ContextMap, blob: Blob, keyId: Int, block: PropertyBlock) = {
    val bid = ic.get[BlobStorage].save(blob);
    block.setValueBlocks(BlobIO._pack(BlobFactory.makeEntry(bid, blob), keyId));
  }

  override def deleteBlobArrayProperty(ic: ContextMap, blobs: BlobArray): Unit = {
    /*
    blobs.value().foreach(blob =>
      ic.get[BlobStorage].delete(blob.asInstanceOf[ManagedBlob].id))
     */
    val gid = blobs.groupId()
    if (gid != null) {
      ic.get[BlobStorage].deleteGroup(gid)
    }
  }

  override def deleteBlobProperty(ic: ContextMap, block: PropertyBlock): Unit = {
    val entry = BlobIO.unpack(block.getValueBlocks);
    ic.get[BlobStorage].delete(entry.id);
  }

  /*
  def readBlobArray(ic: ContextMap, dataBuffer: ByteBuffer, arrayLength: Int): Array[Blob] = {
    (0 to arrayLength - 1).map { x =>
      val byteLength = dataBuffer.getInt();
      val blobByteArray = new Array[Byte](byteLength);
      dataBuffer.get(blobByteArray);
      StoreBlobIO.readBlob(ic, blobByteArray);
    }.toArray
  }
   */

  override def readBlobArray(ic: ContextMap, dataBuffer: ByteBuffer, arrayLength: Int): BlobArray = {
    val bytes = BlobId.EMPTY.asByteArray()
    dataBuffer.get(bytes)
    val bid = BlobId.fromBytes(bytes)
    new BlobArray(bid, new BlobArrayProvider() {
      override def get(): Array[Blob] = ic.get[BlobStorage]().loadGroup(bid).get
    })
  }

  override def readBlobValue(ic: ContextMap, values: Array[Long]): BlobValue = {
    val entry = BlobIO.unpack(values);
    val storage = ic.get[BlobStorage];

    val blob = BlobFactory.makeStoredBlob(entry, new InputStreamSource {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val bid = entry.id;
        storage.load(bid).getOrElse(throw new BlobNotExistException(bid)).offerStream(consume)
      }
    });

    BlobValue(blob);
  }
}

object StoreBlobIO extends Logging {
  val service = ServiceLoader.load[StoreBlobIOService](classOf[StoreBlobIOService]).iterator().next()

  def saveBlobArray(buf: ByteBuffer, blobs: Array[Blob], ic: ContextMap) = {
    service.saveBlobArray(buf, blobs, ic)
  }

  def saveBlob(ic: ContextMap, blob: Blob, keyId: Int, block: PropertyBlock) = {
    service.saveBlob(ic, blob, keyId, block)
  }

  def deleteBlobArrayProperty(ic: ContextMap, blobs: BlobArray): Unit = {
    service.deleteBlobArrayProperty(ic, blobs)
  }

  def deleteBlobProperty(ic: ContextMap, block: PropertyBlock): Unit = {
    service.deleteBlobProperty(ic, block)
  }

  def readBlob(ic: ContextMap, bytes: Array[Byte]): Blob = {
    service.readBlobValue(ic, StreamUtils.convertByteArray2LongArray(bytes)).blob;
  }

  def readBlobArray(ic: ContextMap, dataBuffer: ByteBuffer, arrayLength: Int): BlobArray = {
    service.readBlobArray(ic, dataBuffer, arrayLength)
  }

  def readBlobValue(ic: ContextMap, block: PropertyBlock): BlobValue = {
    service.readBlobValue(ic, block.getValueBlocks);
  }

  def readBlobValue(ic: ContextMap, values: Array[Long]): BlobValue = {
    service.readBlobValue(ic, values)
  }
}

class BlobNotExistException(bid: BlobId) extends RuntimeException {

}