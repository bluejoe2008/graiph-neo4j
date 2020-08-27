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

import java.io.{DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream, InputStream}
import java.util.UUID

import org.apache.commons.io.IOUtils
import org.neo4j.blob._
import org.neo4j.blob.impl.{BlobFactory, MimeTypeFactory}
import org.neo4j.blob.util.ConfigUtils._
import org.neo4j.blob.util.{Configuration, GlobalContext, Logging}

trait Closable {
  def initialize(storeDir: File, conf: Configuration): Unit;

  def disconnect(): Unit;
}

trait BlobStorage extends Closable {
  def save(blob: Blob): BlobId

  def load(bid: BlobId): Option[Blob]

  def delete(bid: BlobId): Unit

  def loadGroup(gid: BlobId): Option[Array[Blob]]

  def saveGroup(blobs: Array[Blob]): BlobId

  def deleteGroup(gid: BlobId): Unit
}

object BlobStorage extends Logging {
  def create(conf: Configuration): BlobStorage =
    create(conf.getRaw("blob.storage"));

  def create(blobStorageClassName: Option[String]): BlobStorage = {
    blobStorageClassName.map(Class.forName(_).newInstance().asInstanceOf[BlobStorage])
      .getOrElse(createDefault())
  }

  def createDefault(): BlobStorage = {
    //will read "default-blob-value-storage-class" entry first
    GlobalContext.getOption("default-blob-value-storage-class")
      .map(Class.forName(_).newInstance().asInstanceOf[BlobStorage])
      .getOrElse(new LocalFileSystemBlobValueStorage())
  }
}

class LocalFileSystemBlobValueStorage extends BlobStorage with Logging {
  var _rootDir: File = _;
  private final val HEADER_SINGLE_BLOB_STORE: Byte = 0
  private final val HEADER_BLOB_ARRAY_STORE: Byte = 1

  override def save(blob: Blob): BlobId = {
    val bid = generateId();
    val file = locateFile(bid);
    file.getParentFile.mkdirs();

    val fos = new DataOutputStream(new FileOutputStream(file))
    fos.writeByte(HEADER_SINGLE_BLOB_STORE);
    fos.write(bid.asByteArray());
    fos.writeLong(blob.mimeType.code);
    fos.writeLong(blob.length);

    blob.offerStream { bis =>
      IOUtils.copy(bis, fos);
    }
    fos.close();

    if (logger.isDebugEnabled()) {
      logger.debug(s"saved a blob in ${file.getAbsoluteFile.getCanonicalPath}")
    }

    bid
  }

  override def load(id: BlobId): Option[Blob] = {
    val blobFile = locateFile(id)
    if (blobFile.exists()) {
      val fis = new DataInputStream(new FileInputStream(blobFile))
      fis.skip(1 + 16) //1B flag + 16B BlobId
      val mimeType = MimeTypeFactory.fromCode(fis.readLong());
      val length = fis.readLong();
      fis.close();

      val blob = BlobFactory.fromInputStreamSource(new InputStreamSource() {
        def offerStream[T](consume: (InputStream) => T): T = {
          val is = new FileInputStream(blobFile);
          //NOTE: skip
          is.skip(1 + 16 + 8 + 8) //1B flag + 16B BlobId + 8B mimeType + 8B blob length
          val t = consume(is);
          is.close();
          t;
        }
      }, length, Some(mimeType))
      Some(blob)
    }
    else {
      None
    }
  }

  override def delete(id: BlobId): Unit = {
    val file = locateFile(id)
    file.delete()
    if (logger.isDebugEnabled()) {
      logger.debug(s"deleted a blob in ${file.getAbsoluteFile.getCanonicalPath}")
    }
  }

  private def generateId(): BlobId = {
    val uuid = UUID.randomUUID()
    new BlobId(uuid.getMostSignificantBits, uuid.getLeastSignificantBits);
  }

  private def locateFile(bid: BlobId): File = {
    val idname = bid.asLiteralString();
    new File(_rootDir, s"${idname.substring(28, 32)}/$idname");
  }

  override def initialize(storeDir: File, conf: Configuration): Unit = {
    val baseDir: File = storeDir; //new File(conf.getRaw("unsupported.dbms.directories.neo4j_home").get());
    _rootDir = conf.getAsFile("blob.storage.file.dir", baseDir, new File(baseDir, "/blob"));
    _rootDir.mkdirs();
    if (logger.isInfoEnabled)
      logger.info(s"using storage dir: ${_rootDir.getCanonicalPath}");
  }

  override def disconnect(): Unit = {
  }

  override def loadGroup(gid: BlobId): Option[Array[Blob]] = {
    val blobFile = locateFile(gid)
    if (blobFile.exists()) {
      val fis = new DataInputStream(new FileInputStream(blobFile))
      fis.skip(1 + 16) //1B flag + 16B BlobId
      val arrlen = fis.readInt()
      var offset: Long = 1 + 16 + 4 //1B flag + 16B BlobId + 4B array length
      offset += arrlen * (8 + 8) //8B mimeType + 8B length

      val entries = (0 to arrlen - 1).toArray.map { i =>
        val mimeType = MimeTypeFactory.fromCode(fis.readLong())
        val length = fis.readLong()
        val offset0 = offset
        offset += length //8B mimeType + 8B length
        (i, mimeType, length, offset0)
      }

      Some(entries.map {
        x =>
          x match {
            case (i: Int, mimeType: MimeType, length: Long, offset: Long) =>
              BlobFactory.fromInputStreamSource(new InputStreamSource() {
                def offerStream[T](consume: (InputStream) => T): T = {
                  val is = new FileInputStream(blobFile);
                  //NOTE: skip
                  is.skip(offset)
                  val t = consume(new InputStream {
                    var nread = 0

                    override def read(): Int = {
                      if (nread >= length)
                        -1
                      else {
                        val byte = is.read()
                        if (byte != -1) {
                          nread += 1
                        }
                        byte
                      }
                    }
                  })
                  is.close()
                  t
                }
              }, length, Some(mimeType))
          }
      })
    }
    else {
      None
    }
  }

  override def saveGroup(blobs: Array[Blob]): BlobId = {
    val bid = generateId();
    val file = locateFile(bid);
    file.getParentFile.mkdirs();

    val fos = new DataOutputStream(new FileOutputStream(file))
    fos.writeByte(HEADER_BLOB_ARRAY_STORE);
    fos.write(bid.asByteArray());
    fos.writeInt(blobs.length);

    blobs.foreach { blob =>
      fos.writeLong(blob.mimeType.code)
      fos.writeLong(blob.length)
    }

    blobs.foreach { blob =>
      blob.offerStream { bis =>
        IOUtils.copy(bis, fos);
      }
    }

    fos.close()

    if (logger.isDebugEnabled()) {
      logger.debug(s"saved ${blobs.size} blobs in ${file.getAbsoluteFile.getCanonicalPath}")
    }

    bid
  }

  override def deleteGroup(gid: BlobId): Unit = {
    val file = locateFile(gid)
    file.delete()
    if (logger.isDebugEnabled()) {
      logger.debug(s"deleted blobs in ${file.getAbsoluteFile.getCanonicalPath}")
    }
  }
}