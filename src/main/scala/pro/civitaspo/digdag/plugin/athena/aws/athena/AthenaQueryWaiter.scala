package pro.civitaspo.digdag.plugin.athena.aws.athena


import com.typesafe.scalalogging.LazyLogging
import io.digdag.util.DurationParam
import software.amazon.awssdk.services.athena.model.{GetQueryExecutionRequest, QueryExecutionState}


case class AthenaQueryWaiter(athena: Athena,
                             successStats: Seq[QueryExecutionState],
                             failureStats: Seq[QueryExecutionState],
                             timeout: DurationParam)
    extends LazyLogging
{
    // ref. https://aws.amazon.com/jp/blogs/architecture/exponential-backoff-and-jitter/
    private val baseSeconds: Double = 1.0
    private val capSeconds: Double = 30.0

    def wait(executionId: String): Unit =
    {
        val req = GetQueryExecutionRequest.builder().queryExecutionId(executionId).build()
        val startedAt = System.currentTimeMillis()
        var attempts = 0

        while (true) {
            val result = athena.withAthena(_.getQueryExecution(req))
            // NOTE: Query string is noisy for logging, so suppress the query string logging.
            logger.info(s"Waiting the query: ${result.queryExecution().toBuilder.query(null).build().toString}")

            val state = result.queryExecution().status().state()
            if (successStats.contains(state)) return
            if (failureStats.contains(state)) {
                throw new IllegalStateException(s"Query execution failed with state: $state (executionId=$executionId)")
            }
            if ((System.currentTimeMillis() - startedAt) >= timeout.getDuration.toMillis) {
                throw new IllegalStateException(s"Query execution timed out after ${timeout.getDuration} (executionId=$executionId)")
            }

            val sleepMs = (math.random() * math.min(capSeconds, baseSeconds * math.pow(2.0, attempts)) * 1000).toLong
            Thread.sleep(sleepMs)
            attempts += 1
        }
    }
}
