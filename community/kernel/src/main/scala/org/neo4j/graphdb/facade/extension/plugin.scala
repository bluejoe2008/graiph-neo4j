package org.neo4j.graphdb.facade.extension

import java.io.File
import java.util.ServiceLoader
import java.util.function.Consumer

import org.neo4j.blob.util.{Configuration, ContextMap, Logging}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.Neo4jConfigUtils._
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.lifecycle.Lifecycle

import scala.collection.mutable.ArrayBuffer

case class DatabaseLifecyclePluginContext(proceduresService: Procedures,
                                          storeDir: File,
                                          neo4jConf: Config,
                                          databaseInfo: DatabaseInfo,
                                          configuration: Configuration,
                                          instanceContext: ContextMap) {

}

trait DatabaseLifecyclePlugin {
  def init(ctx: DatabaseLifecyclePluginContext)

  def start(ctx: DatabaseLifecyclePluginContext)

  def stop(ctx: DatabaseLifecyclePluginContext)
}

class ExtendedDatabaseLifecyclePluginsService(proceduresService: Procedures, storeDir: File, neo4jConf: Config, databaseInfo: DatabaseInfo)
  extends Lifecycle with Logging {

  val configuration: Configuration = neo4jConf;
  val ctx = new DatabaseLifecyclePluginContext(proceduresService, storeDir, neo4jConf, databaseInfo, configuration, neo4jConf.getInstanceContext);

  /////binds context
  neo4jConf.getInstanceContext.put[ExtendedDatabaseLifecyclePluginsService](this);

  override def shutdown(): Unit = {
  }

  val plugins = ArrayBuffer[DatabaseLifecyclePlugin]()

  ServiceLoader.load(classOf[DatabaseLifecyclePlugin]).forEach(new Consumer[DatabaseLifecyclePlugin]() {
    override def accept(t: DatabaseLifecyclePlugin): Unit = {
      if (logger.isDebugEnabled())
        logger.debug(s"loading database lifecycle plugin: $t")

      plugins += t
    }
  })

  override def init(): Unit = plugins.foreach(_.init(ctx))

  override def start(): Unit = plugins.foreach(_.start(ctx))

  override def stop(): Unit = plugins.foreach(_.stop(ctx))
}
