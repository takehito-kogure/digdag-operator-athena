package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue
import software.amazon.awssdk.services.glue.model.{DeleteTableRequest, GetTableRequest, GetTablesRequest, Table}

import scala.jdk.CollectionConverters._
import scala.util.Try


case class TableCatalog(glue: Glue)
    extends java.io.Closeable
{
    private val tableCache = scala.collection.mutable.Map.empty[String, Table]

    override def close(): Unit = tableCache.clear()

    def describe(catalogIdOption: Option[String],
                 database: String,
                 table: String): Table =
    {
        val key = s"${catalogIdOption.getOrElse("")}/$database/$table"
        tableCache.getOrElseUpdate(key, {
            val builder = GetTableRequest.builder()
                .databaseName(database)
                .name(table)
            catalogIdOption.foreach(builder.catalogId)
            glue.withGlue(_.getTable(builder.build())).table()
        })
    }

    def isPartitioned(catalogIdOption: Option[String],
                      database: String,
                      table: String): Boolean =
    {
        !describe(catalogIdOption, database, table).partitionKeys().isEmpty
    }

    def exists(catalogIdOption: Option[String],
               database: String,
               table: String): Boolean =
    {
        Try(describe(catalogIdOption, database, table)).isSuccess
    }

    def delete(catalogIdOption: Option[String],
               database: String,
               table: String): Unit =
    {
        val builder = DeleteTableRequest.builder()
            .databaseName(database)
            .name(table)
        catalogIdOption.foreach(builder.catalogId)
        glue.withGlue(_.deleteTable(builder.build()))
    }

    def list(catalogIdOption: Option[String],
             database: String,
             expression: Option[String] = None,
             limit: Option[Int] = None): Seq[Table] =
    {
        val builder = GetTablesRequest.builder().databaseName(database)
        catalogIdOption.foreach(builder.catalogId)
        expression.foreach(builder.expression)
        limit.foreach(l => builder.maxResults(l))

        def recursiveGetTables(nextToken: Option[String] = None,
                               lastTables: Seq[Table] = Seq()): Seq[Table] =
        {
            nextToken.foreach(builder.nextToken)
            val results = glue.withGlue(_.getTables(builder.build()))
            val tables = lastTables ++ results.tableList().asScala.toSeq
            limit.foreach { i =>
                if (tables.length >= i) return tables.slice(0, i)
            }
            Option(results.nextToken()) match {
                case Some(nt) => recursiveGetTables(nextToken = Option(nt), lastTables = tables)
                case None     => tables
            }
        }

        recursiveGetTables()
    }
}
