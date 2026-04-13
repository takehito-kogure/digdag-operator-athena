package pro.civitaspo.digdag.plugin.athena.aws.athena


import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model._

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.chaining._


case class Athena(aws: Aws)
    extends AwsService(aws)
        with java.io.Closeable
{
    val DEFAULT_WORKGROUP = "primary"
    lazy val DEFAULT_OUTPUT_LOCATION: String = {
        val accountId = aws.sts.getCallerIdentityAccountId
        s"s3://aws-athena-query-results-$accountId-${aws.region}/"
    }

    private var athenaClientOpt: Option[AthenaClient] = None

    private def athenaClient: AthenaClient = athenaClientOpt.getOrElse {
        val b = aws.configureClient(AthenaClient.builder())
        aws.httpClientOption.foreach(b.httpClient)
        val c = b.build()
        athenaClientOpt = Some(c)
        c
    }

    override def close(): Unit =
    {
        athenaClientOpt.foreach(_.close())
        athenaClientOpt = None
    }

    def withAthena[A](f: AthenaClient => A): A = f(athenaClient)

    def startQueryExecution(query: String,
                            database: Option[String] = None,
                            workGroup: Option[String] = None,
                            outputLocation: Option[String] = None,
                            requestToken: Option[String] = None): String =
    {
        val builder = StartQueryExecutionRequest.builder()
            .queryString(query)
            .workGroup(workGroup.getOrElse(DEFAULT_WORKGROUP))
            .resultConfiguration(ResultConfiguration.builder()
                                     .outputLocation(resolveWorkGroupOutputLocation(workGroup.getOrElse(DEFAULT_WORKGROUP)))
                                     .build())
        database.foreach(db => builder.queryExecutionContext(QueryExecutionContext.builder().database(db).build()))
        requestToken.foreach(builder.clientRequestToken)

        withAthena(_.startQueryExecution(builder.build())).queryExecutionId()
    }

    def resolveWorkGroupOutputLocation(workGroup: String): String =
    {
        workGroup match {
            case DEFAULT_WORKGROUP => DEFAULT_OUTPUT_LOCATION
            case wg                =>
                val t = Try {
                    withAthena(_.getWorkGroup(GetWorkGroupRequest.builder().workGroup(wg).build()))
                        .workGroup()
                        .configuration()
                        .resultConfiguration()
                        .outputLocation()
                }
                t match {
                    case Success(outputLocation) => outputLocation
                    case Failure(ex)             => DEFAULT_OUTPUT_LOCATION.tap { default =>
                        logger.warn(s"Use $default as athena output location because the workgroup output location cannot be resolved due to '${ex.getMessage}'.", ex)
                    }
                }
        }
    }

    def getQueryExecution(executionId: String): QueryExecution =
    {
        withAthena(_.getQueryExecution(GetQueryExecutionRequest.builder().queryExecutionId(executionId).build())).queryExecution()
    }

    def waitQueryExecution(executionId: String,
                           successStates: Seq[QueryExecutionState],
                           failureStates: Seq[QueryExecutionState],
                           timeout: DurationParam): Unit =
    {
        val waiter = AthenaQueryWaiter(athena = this,
                                       successStats = successStates,
                                       failureStats = failureStates,
                                       timeout = timeout)
        waiter.wait(executionId)
    }

    def runQuery(query: String,
                 database: Option[String] = None,
                 workGroup: Option[String] = None,
                 outputLocation: Option[String] = None,
                 requestToken: Option[String] = None,
                 successStates: Seq[QueryExecutionState],
                 failureStates: Seq[QueryExecutionState],
                 timeout: DurationParam): QueryExecution =
    {
        val executionId: String = startQueryExecution(query = query,
                                                      database = database,
                                                      workGroup = workGroup,
                                                      outputLocation = outputLocation,
                                                      requestToken = requestToken)

        val t = Try {
            waitQueryExecution(executionId = executionId,
                               successStates = successStates,
                               failureStates = failureStates,
                               timeout = timeout)
        }
        t match {
            case Success(_)         => logger.info(s"Success to execute the query: $executionId")
            case Failure(exception) =>
                logger.error(exception.getMessage, exception)
                val qe = getQueryExecution(executionId = executionId)
                throw new IllegalStateException(s"Failed the query execution: ${qe.toBuilder.query(null).build().toString}", exception)
        }

        getQueryExecution(executionId = executionId)
    }

    def preview(executionId: String,
                limit: Int): ResultSet =
    {
        def requestRecursive(nextToken: Option[String] = None): ResultSet =
        {
            val builder = GetQueryResultsRequest.builder()
                .queryExecutionId(executionId)
                .maxResults(limit)
            nextToken.foreach(builder.nextToken)

            val res = withAthena(_.getQueryResults(builder.build()))
            val rows = res.resultSet().rows().asScala.toBuffer

            Option(res.nextToken()).foreach { token =>
                val next = requestRecursive(Option(token))
                rows ++= next.rows().asScala
            }

            res.resultSet().toBuilder.rows(rows.asJava).build()
        }

        requestRecursive()
    }
}
