package pro.civitaspo.digdag.plugin.athena.aws.glue


import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}
import pro.civitaspo.digdag.plugin.athena.aws.glue.catalog.{DatabaseCatalog, PartitionCatalog, TableCatalog}
import software.amazon.awssdk.services.glue.GlueClient


case class Glue(aws: Aws)
    extends AwsService(aws)
        with java.io.Closeable
{
    private var glueClientOpt: Option[GlueClient] = None

    private def glueClient: GlueClient = glueClientOpt.getOrElse {
        val b = aws.configureClient(GlueClient.builder())
        aws.httpClientOption.foreach(b.httpClient)
        val c = b.build()
        glueClientOpt = Some(c)
        c
    }

    override def close(): Unit = glueClientOpt.foreach(_.close())

    def withGlue[A](f: GlueClient => A): A = f(glueClient)

    // Use catalog api https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog.html
    val database: DatabaseCatalog = catalog.DatabaseCatalog(glue = this)
    val table: TableCatalog = catalog.TableCatalog(glue = this)
    val partition: PartitionCatalog = catalog.PartitionCatalog(glue = this)
}
