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
package org.neo4j.kernel.impl

import java.util.concurrent.atomic.AtomicInteger
import org.neo4j.blob._
import org.neo4j.blob.util.ReflectUtils._
import org.neo4j.blob.util._
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.CloseListener
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, TopLevelTransaction}
import org.apache.commons.codec.digest.DigestUtils
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by bluejoe on 2019/4/3.
  */
object KernelTransactionEventHub {
  private val _local = new ThreadLocal[TopLevelTransaction]()
  private val _rand = new Random()
  private val _listeners = ArrayBuffer[KernelTransactionEventListener]()
  private val _serial = new AtomicInteger((System.currentTimeMillis() >> 32).toInt);

  def addListener(listener: KernelTransactionEventListener) = _listeners += listener;

  private def _notify(event: KernelTransactionEvent) = _listeners.foreach(_.notified(event))

  private def _transactionId(kt: TopLevelTransaction) = "" + kt.hashCode();

  def currentTopLevelTransaction() = _local.get();

  private def currentTopLevelTransactionId() = _transactionId(_local.get());

  def cacheBlob(blob: Blob) = {
    val handle = DigestUtils.md5Hex(StreamUtils.covertLong2ByteArray(_serial.getAndIncrement() << 8 | _rand.nextInt(128)))
    _notify(TransactionalBlobCached(handle, blob, currentTopLevelTransactionId()))
    handle
  }

  def notifyTransactionBegan(tx: InternalTransaction) = {
    tx match {
      case topTx: TopLevelTransaction =>
        val kt = topTx._get("transaction").asInstanceOf[KernelTransaction];
        _local.set(topTx);
        _notify(KernelTransactionBegan(_transactionId(topTx)))

        kt.registerCloseListener(new CloseListener {
          override def notify(txId: Long): Unit = {
            _notify(KernelTransactionClosed(_transactionId(topTx)))
            _local.remove();
          }
        })

      case _ => {

      }
    }
  }
}

trait KernelTransactionEvent {

}

trait KernelTransactionEventListener {
  def notified(event: KernelTransactionEvent)
}

case class KernelTransactionBegan(transactionId: String) extends KernelTransactionEvent {

}

case class KernelTransactionClosed(transactionId: String) extends KernelTransactionEvent {

}

case class TransactionalBlobCached(handle: String, blob: Blob, transactionId: String) extends KernelTransactionEvent {

}