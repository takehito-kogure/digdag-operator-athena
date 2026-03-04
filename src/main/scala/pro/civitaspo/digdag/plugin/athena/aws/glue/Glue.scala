package pro.civitaspo.digdag.plugin.athena.aws.glue


import com.amazonaws.services.glue.{AWSGlue, AWSGlueClientBuilder}
import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}
import pro.civitaspo.digdag.plugin.athena.aws.glue.catalog.{DatabaseCatalog, PartitionCatalog, TableCatalog}


case class Glue(aws: Aws)
    extends AwsService(aws)
        with java.io.Closeable
{
    private var glueClientOpt: Option[AWSGlue] = None

    private def glueClient: AWSGlue = glueClientOpt.getOrElse {
        val c = aws.buildService(AWSGlueClientBuilder.standard())
        glueClientOpt = Some(c)
        c
    }

    override def close(): Unit = glueClientOpt.foreach(_.shutdown())

    def withGlue[A](f: AWSGlue => A): A = f(glueClient)

    // Use catalog api https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog.html
    val database: DatabaseCatalog = catalog.DatabaseCatalog(glue = this)
    val table: TableCatalog = catalog.TableCatalog(glue = this)
    val partition: PartitionCatalog = catalog.PartitionCatalog(glue = this)
}
