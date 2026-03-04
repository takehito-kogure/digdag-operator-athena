package pro.civitaspo.digdag.plugin.athena.drop_table_multi


import java.time.Instant

import software.amazon.awssdk.services.glue.model.Table
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator


class AthenaDropTableMultiOperator(operatorName: String,
                                   context: OperatorContext,
                                   systemConfig: Config,
                                   templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    val database: String = params.get("database", classOf[String])
    val regexp: String = params.getOptional("regexp", classOf[String]).orNull()
    val protect: Option[Config] = Option(params.getOptionalNested("protect").orNull())
    val limit: Option[Int] = Option(params.getOptional("limit", classOf[Int]).orNull())
    val withLocation: Boolean = params.get("with_location", classOf[Boolean], false)
    val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

    val now: Long = System.currentTimeMillis()

    override def runTask(): TaskResult =
    {
        logger.info(s"Drop tables matched by the expression: /$regexp/ in $database")
        aws.glue.table.list(catalogId, database, Option(regexp), limit).foreach { t =>
            if (!isProtected(t)) {
                if (withLocation) {
                    val location: String = {
                        val l = t.storageDescriptor.location
                        if (l.endsWith("/")) l
                        else l + "/"
                    }
                    if (aws.s3.hasObjects(location)) {
                        logger.info(s"Delete objects because the location $location has objects.")
                        aws.s3.rm_r(location).foreach(uri => logger.info(s"Deleted: ${uri.toString}"))
                    }
                }
                logger.info(s"Drop the table '$database.${t.name}'")
                aws.glue.table.delete(catalogId, database, t.name)
            }
        }
        TaskResult.empty(cf)
    }

    protected def isProtected(t: Table): Boolean =
    {
        protect match {
            case None    => return false
            case Some(c) =>
                Option(c.getOptional("created_within", classOf[DurationParam]).orNull()) match {
                    case None    => // do nothing
                    case Some(d) =>
                        Option(t.createTime).foreach { instant =>
                            if (isInstantWithin(instant, d)) {
                                logger.info(s"Protect the table ${t.databaseName}.${t.name} because this is created" +
                                                s" within ${d.toString} (created at ${t.createTime.toString}).")
                                return true
                            }
                        }
                }
                Option(c.getOptional("updated_within", classOf[DurationParam]).orNull()) match {
                    case None    => // do nothing
                    case Some(d) =>
                        Option(t.updateTime).foreach { instant =>
                            if (isInstantWithin(instant, d)) {
                                logger.info(s"Protect the table ${t.databaseName}.${t.name} because this is updated" +
                                                s" within ${d.toString} (updated at ${t.updateTime.toString}).")
                                return true
                            }
                        }
                }
                Option(c.getOptional("accessed_within", classOf[DurationParam]).orNull()) match {
                    case None    => // do nothing
                    case Some(d) =>
                        Option(t.lastAccessTime).foreach { instant =>
                            if (isInstantWithin(instant, d)) {
                                logger.info(s"Protect the table ${t.databaseName}.${t.name} because this is accessed" +
                                                s" within ${d.toString} (last accessed at ${t.lastAccessTime.toString}).")
                                return true
                            }
                        }
                }
        }
        false
    }

    protected def isInstantWithin(target: Instant,
                                  durationWithin: DurationParam): Boolean =
    {
        val instantWithin: Instant = Instant.ofEpochMilli(now - durationWithin.getDuration.toMillis)
        target.isAfter(instantWithin)
    }

}
