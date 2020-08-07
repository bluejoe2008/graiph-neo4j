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
import org.neo4j.blob.util._
import org.neo4j.kernel.impl.{KernelTransactionClosed, KernelTransactionEvent, KernelTransactionEventListener, TransactionalBlobCached}

import scala.collection.mutable

/**
  * Created by bluejoe on 2019/4/3.
  */
//TODO: use BoltTransaction?
class LaterAccessBlobCache extends KernelTransactionEventListener with Logging {
  //3m
  val MAX_ALIVE = 3 * 60 * 1000

  //5s
  val CHECK_INTERVAL = 5 * 1000

  case class Entry(id: String, blob: Blob, transactionId: String, time: Long = System.currentTimeMillis()) {
    val expired = time + MAX_ALIVE
  }

  val _blobCache = mutable.Map[String, Entry]();

  //expired blob checking thread
  new Thread(new Runnable() {
    override def run(): Unit = {
      while (true) {
        Thread.sleep(CHECK_INTERVAL);
        if (!_blobCache.isEmpty) {
          val now = System.currentTimeMillis()
          val handles = _blobCache.filter(_._2.expired < now).map(_._1)
          if (!handles.isEmpty) {
            if (logger.isDebugEnabled())
              logger.debug(s"invalidated: ${handles.toList}")
            _blobCache --= handles
          }
        }
      }
    }
  }).start()

  def get(handle: String): Option[Blob] = {
    _blobCache.get(handle).map(_.blob)
  }

  override def notified(event: KernelTransactionEvent): Unit = event match {
    case TransactionalBlobCached(handle: String, blob: Blob, transactionId: String) =>
      val entry = Entry(handle, blob, transactionId)
      _blobCache += handle -> entry

    case KernelTransactionClosed(transactionId: String) =>
      _blobCache --= _blobCache.filter(_._2.transactionId.equals(transactionId)).map(_._1);

    case _ =>
  }
}