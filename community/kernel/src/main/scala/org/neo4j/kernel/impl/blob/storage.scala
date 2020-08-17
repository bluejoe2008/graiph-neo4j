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

import java.io.{File, FileInputStream, FileOutputStream, InputStream}
import java.util.UUID

import org.apache.commons.io.IOUtils
import org.neo4j.blob._
import org.neo4j.blob.impl.{BlobFactory, BlobIdFactory, MimeTypeFactory}
import org.neo4j.blob.util.ConfigUtils._
import org.neo4j.blob.util.StreamUtils._
import org.neo4j.blob.util.{Configuration, GlobalContext, Logging}

trait Closable {
  def initialize(storeDir: File, conf: Configuration): Unit;

  def disconnect(): Unit;
}

trait BlobStorage extends Closable {
  def save(blob: Blob): BlobId;

  def load(id: BlobId): Option[Blob];

  def delete(id: BlobId): Unit;
}

object BlobStorage extends Logging {
  def create(conf: Configuration): BlobStorage =
    create(conf.getRaw("blob.storage"));

  def create(blobStorageClassName: Option[String]): BlobStorage = {
    blobStorageClassName.map(Class.forName(_).newInstance().asInstanceOf[BlobStorage])
      .getOrElse(createDefault())
  }

  class DefaultLocalFileSystemBlobValueStorage extends BlobStorage with Logging {
    var _rootDir: File = _;

    override def save(blob: Blob): BlobId = {
      val bid = generateId();
      val file = locateFile(bid);
      file.getParentFile.mkdirs();

      val fos = new FileOutputStream(file);
      fos.write(bid.asByteArray());
      fos.writeLong(blob.mimeType.code);
      fos.writeLong(blob.length);

      blob.offerStream { bis =>
        IOUtils.copy(bis, fos);
      }
      fos.close();

      bid;
    }

    override def load(id: BlobId): Option[Blob] = {
      val file = locateFile(id)
      if (file.exists())
        Some(readFromBlobFile(file)._2)
      else
        None
    }

    override def delete(id: BlobId): Unit =
      locateFile(id).delete()

    private def generateId(): BlobId = {
      val uuid = UUID.randomUUID()
      BlobId(uuid.getMostSignificantBits, uuid.getLeastSignificantBits);
    }

    private def locateFile(bid: BlobId): File = {
      val idname = bid.asLiteralString();
      new File(_rootDir, s"${idname.substring(28, 32)}/$idname");
    }

    private def readFromBlobFile(blobFile: File): (BlobId, Blob) = {
      val fis = new FileInputStream(blobFile);
      val blobId = BlobIdFactory.readFromStream(fis);
      val mimeType = MimeTypeFactory.fromCode(fis.readLong());
      val length = fis.readLong();
      fis.close();

      val blob = BlobFactory.fromInputStreamSource(new InputStreamSource() {
        def offerStream[T](consume: (InputStream) => T): T = {
          val is = new FileInputStream(blobFile);
          //NOTE: skip
          is.skip(8 * 4);
          val t = consume(is);
          is.close();
          t;
        }
      }, length, Some(mimeType));

      (blobId, blob);
    }

    override def initialize(storeDir: File, conf: Configuration): Unit = {
      val baseDir: File = storeDir; //new File(conf.getRaw("unsupported.dbms.directories.neo4j_home").get());
      _rootDir = conf.getAsFile("blob.storage.file.dir", baseDir, new File(baseDir, "/blob"));
      _rootDir.mkdirs();
      logger.info(s"using storage dir: ${_rootDir.getCanonicalPath}");
    }

    override def disconnect(): Unit = {
    }
  }

  def createDefault(): BlobStorage = {
    //will read "default-blob-value-storage-class" entry first
    GlobalContext.getOption("default-blob-value-storage-class")
      .map(Class.forName(_).newInstance().asInstanceOf[BlobStorage])
      .getOrElse(new DefaultLocalFileSystemBlobValueStorage())
  }
}