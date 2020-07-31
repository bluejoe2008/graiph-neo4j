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

import java.io.File

import org.neo4j.blob.util.Logging
import org.neo4j.graphdb.facade.extension.{DatabaseLifecyclePlugin, DatabaseLifecyclePluginContext}
import org.neo4j.graphdb.factory.GraphDatabaseSettings

class BlobStoragePlugin extends DatabaseLifecyclePlugin with Logging {
  var blobStorage: BlobStorage = _;

  override def init(ctx: DatabaseLifecyclePluginContext): Unit = {
    blobStorage = BlobStorage.create(ctx.configuration);
    ctx.instanceContext.put[BlobStorage](blobStorage);
  }

  override def stop(ctx: DatabaseLifecyclePluginContext): Unit = {
    blobStorage.disconnect();
    logger.info(s"blob storage disconnected: $blobStorage");
  }

  override def start(ctx: DatabaseLifecyclePluginContext): Unit = {
    blobStorage.initialize(new File(ctx.storeDir,
      ctx.neo4jConf.get(GraphDatabaseSettings.active_database)),
      ctx.configuration);
  }
}