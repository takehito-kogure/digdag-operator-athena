package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import io.digdag.client.config.ConfigException
import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue
import software.amazon.awssdk.services.glue.model.{Column, CreatePartitionRequest, DeletePartitionRequest, GetPartitionRequest, Partition, PartitionInput, StorageDescriptor, UpdatePartitionRequest}

import scala.jdk.CollectionConverters._
import scala.util.Try


case class PartitionCatalog(glue: Glue)
{
    def add(catalogIdOption: Option[String] = None,
            database: String,
            table: String,
            partitionKv: Map[String, String],
            locationOption: Option[String] = None): Unit =
    {
        val pi = newPartitionInput(catalogIdOption, database, table, partitionKv, locationOption)

        val builder = CreatePartitionRequest.builder()
            .databaseName(database)
            .tableName(table)
            .partitionInput(pi)
        catalogIdOption.foreach(builder.catalogId)

        glue.withGlue(_.createPartition(builder.build()))
    }

    def update(catalogIdOption: Option[String] = None,
               database: String,
               table: String,
               partitionKv: Map[String, String],
               locationOption: Option[String] = None): Unit =
    {
        val pi = newPartitionInput(catalogIdOption, database, table, partitionKv, locationOption)

        val builder = UpdatePartitionRequest.builder()
            .databaseName(database)
            .tableName(table)
            .partitionValueList(pi.values())
            .partitionInput(pi)
        catalogIdOption.foreach(builder.catalogId)

        glue.withGlue(_.updatePartition(builder.build()))
    }

    def delete(catalogIdOption: Option[String] = None,
               database: String,
               table: String,
               partitionKv: Map[String, String]): Unit =
    {
        val pi = newPartitionInput(catalogIdOption, database, table, partitionKv)

        val builder = DeletePartitionRequest.builder()
            .databaseName(database)
            .tableName(table)
            .partitionValues(pi.values())
        catalogIdOption.foreach(builder.catalogId)

        glue.withGlue(_.deletePartition(builder.build()))
    }

    def describe(catalogIdOption: Option[String] = None,
                 database: String,
                 table: String,
                 partitionKv: Map[String, String]): Partition =
    {
        val pi = newPartitionInput(catalogIdOption, database, table, partitionKv)

        val builder = GetPartitionRequest.builder()
            .databaseName(database)
            .tableName(table)
            .partitionValues(pi.values())
        catalogIdOption.foreach(builder.catalogId)

        glue.withGlue(_.getPartition(builder.build())).partition()
    }

    def exists(catalogIdOption: Option[String] = None,
               database: String,
               table: String,
               partitionKv: Map[String, String]): Boolean =
    {
        Try(describe(catalogIdOption, database, table, partitionKv)).isSuccess
    }

    private def newPartitionInput(catalogIdOption: Option[String] = None,
                                  database: String,
                                  table: String,
                                  partitionKv: Map[String, String],
                                  locationOption: Option[String] = None): PartitionInput =
    {
        val t = glue.table.describe(catalogIdOption, database, table)
        val sd = t.storageDescriptor()
        val pVals: Seq[String] = t.partitionKeys().asScala.toSeq.map { c =>
            partitionKv.getOrElse(c.name(), throw new ConfigException(
                s"Table[$database.$table] has a column${c.toString} as a partition," +
                    s" but partitionKv {${partitionKv.mkString(",")}} does not have this."
                ))
        }
        val location: String = locationOption.getOrElse {
            val l = Option(t.storageDescriptor().location()).getOrElse {
                throw new IllegalStateException(s"The location of '$database.$table' is null.")
            }
            val sb = new StringBuilder()
            sb.append(l)
            if (!l.endsWith("/")) sb.append("/")
            t.partitionKeys().asScala.zipWithIndex.foreach {
                case (c: Column, i: Int) => sb.append(c.name() + "=" + pVals(i) + "/")
            }
            sb.result()
        }
        PartitionInput.builder()
            .parameters(t.parameters())
            .values(pVals.asJava)
            .storageDescriptor(
                StorageDescriptor.builder()
                    .bucketColumns(sd.bucketColumns())
                    .columns(sd.columns())
                    .compressed(sd.compressed())
                    .inputFormat(sd.inputFormat())
                    .outputFormat(sd.outputFormat())
                    .numberOfBuckets(sd.numberOfBuckets())
                    .parameters(sd.parameters())
                    .serdeInfo(sd.serdeInfo())
                    .skewedInfo(sd.skewedInfo())
                    .sortColumns(sd.sortColumns())
                    .storedAsSubDirectories(sd.storedAsSubDirectories())
                    .location(location)
                    .build())
            .build()
    }

    def generateLocation(catalogIdOption: Option[String] = None,
                         database: String,
                         table: String,
                         partitionKv: Map[String, String]): String =
    {
        val pi = newPartitionInput(catalogIdOption, database, table, partitionKv)
        pi.storageDescriptor().location()
    }
}
