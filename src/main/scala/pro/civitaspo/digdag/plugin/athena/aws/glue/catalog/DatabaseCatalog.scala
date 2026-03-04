package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue
import software.amazon.awssdk.services.glue.model.{Database, GetDatabaseRequest, GetDatabasesRequest}

import scala.jdk.CollectionConverters._
import scala.util.Try


case class DatabaseCatalog(glue: Glue)
{

    def describe(catalogIdOption: Option[String],
                 database: String): Database =
    {
        val builder = GetDatabaseRequest.builder().name(database)
        catalogIdOption.foreach(builder.catalogId)
        glue.withGlue(_.getDatabase(builder.build())).database()
    }

    def exists(catalogIdOption: Option[String],
               database: String): Boolean =
    {
        Try(describe(catalogIdOption, database)).isSuccess
    }

    def list(catalogIdOption: Option[String],
             limit: Option[Int] = None): Seq[Database] =
    {
        val builder = GetDatabasesRequest.builder()
        catalogIdOption.foreach(builder.catalogId)
        limit.foreach(l => builder.maxResults(l))

        def recursiveGetDatabases(nextToken: Option[String] = None,
                                  lastDatabases: Seq[Database] = Seq()): Seq[Database] =
        {
            nextToken.foreach(builder.nextToken)
            val results = glue.withGlue(_.getDatabases(builder.build()))
            val databases = lastDatabases ++ results.databaseList().asScala.toSeq
            limit.foreach { i =>
                if (databases.length >= i) return databases.slice(0, i)
            }
            Option(results.nextToken()) match {
                case Some(nt) => recursiveGetDatabases(nextToken = Option(nt), lastDatabases = databases)
                case None     => databases
            }
        }

        recursiveGetDatabases()
    }

}
