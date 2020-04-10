@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.athena.AthenaAsyncClient
import software.amazon.awssdk.services.athena.AthenaAsyncClientBuilder
import software.amazon.awssdk.services.athena.model.*
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap

//TODO merge with AthenaAsyncKlient
data class AthenaConfig(
        val client: AthenaAsyncKlient,
        val workGroup: String,
        val queryBatchSize: Int = 1000,
        val waitDelaySeed: Long = 500L,
        val waitDelayFunction: (Long) -> Long = { it * 2 },
        val throttleDelaySeed: Long = 200L,
        val throttleDelayFunction: (Long) -> Long = { it * 2 },
        val maxThrottles: Int = Int.MAX_VALUE,
        val debugMode: Boolean = false
)

private fun AthenaConfig.debug(action: () -> Unit) {
    if (debugMode) {
        action()
    }
}

private val mutex = Mutex()

//TODO shall we revise it according to Per Account API Call Quotas?
private suspend fun <T> throttle(config: AthenaConfig,
                                 delay: Long = config.throttleDelaySeed,
                                 throttled: Int = 0,
                                 action: suspend () -> T): T {
    return try {
        action()
    } catch (e: SdkServiceException) {
        if (e.isThrottlingException) {
            if (throttled > 0) {
                config.debug { println("Throttled in mutex: $throttled") }
                if (throttled > config.maxThrottles) {
                    throw IllegalStateException("Too long throttling for the query ($throttled times), try again later")
                } else {
                    println("Caught throttling error, already exclusive, stop all athena queries for " + Duration.of(delay, ChronoUnit.MILLIS))
                    delay(delay)
                    throttle(config, config.throttleDelayFunction(delay), throttled + 1, action)
                }
            } else {
                mutex.withLock {
                    delay(delay)//TODO replace with loop and make inline
                    throttle(config, config.throttleDelayFunction(delay), throttled + 1, action)
                }
            }
        } else {
            throw e
        }
    }
}


suspend fun String.runInAthenaAsync(config: AthenaConfig) = config.let { athena ->
    throttle(config) { athena.client.nativeClient.startQueryExecution { it.workGroup(athena.workGroup).queryString(this) }.await().queryExecutionId() }
}

@ExperimentalCoroutinesApi
suspend fun <T> String.runInAthena(config: AthenaConfig, mapper: (List<Datum>) -> T): Flow<T> = config.let { athena ->
    val string = this
    val executionId = throttle(config) {
        athena.client.nativeClient.startQueryExecution { it.workGroup(athena.workGroup).queryString(string) }.await()
                .queryExecutionId()
    }
    config.debug { println("Started $string") }
    throttle(config) { waitForFinish(athena, executionId) }
    config.debug { println("Finished $string") }
    return athena.client.getQueryResults { it.queryExecutionId(executionId) }.map { coroutineScope { mapper(it) } }
}

@Throws(IllegalStateException::class)
suspend fun waitForFinish(config: AthenaConfig,
                          queryExecutionId: String) = config.let { athena ->
    var status = QueryExecutionStatus.builder().state(QueryExecutionState.QUEUED).build()
    val delaySeq = generateSequence(athena.waitDelaySeed, athena.waitDelayFunction).iterator()
    var firstTime = true
    while (status.state() == QueryExecutionState.RUNNING || status.state() == QueryExecutionState.QUEUED) {
        val delay = delaySeq.next()
        if (!firstTime) {
            config.debug { println("Query $queryExecutionId is still running, waiting for ${Duration.ofMillis(delay)}") }
        }
        delay(delay)
        val future = athena.client.nativeClient.getQueryExecution { it.queryExecutionId(queryExecutionId) }
        status = future.await().queryExecution().status()
        firstTime = false
    }
    if (status.state() != QueryExecutionState.SUCCEEDED) {
        throw IllegalStateException("Invalid query status ${status.state()} due to ${status.stateChangeReason()}")
    }
}

class AthenaAsyncKlient(val nativeClient: AthenaAsyncClient) {
    suspend fun batchGetNamedQuery(builder: (BatchGetNamedQueryRequest.Builder) -> Unit): BatchGetNamedQueryResponse {
        return nativeClient.batchGetNamedQuery(builder).await()
    }

    suspend fun batchGetQueryExecution(builder: (BatchGetQueryExecutionRequest.Builder) -> Unit): BatchGetQueryExecutionResponse {
        return nativeClient.batchGetQueryExecution(builder).await()
    }

    suspend fun createNamedQuery(builder: (CreateNamedQueryRequest.Builder) -> Unit): String {
        return nativeClient.createNamedQuery(builder).await().namedQueryId()
    }

    suspend fun createWorkGroup(builder: (CreateWorkGroupRequest.Builder) -> Unit) {
        nativeClient.createWorkGroup(builder).await()
    }

    suspend fun deleteNamedQuery(builder: (DeleteNamedQueryRequest.Builder) -> Unit) {
        nativeClient.deleteNamedQuery(builder).await()
    }

    suspend fun deleteWorkGroup(builder: (DeleteWorkGroupRequest.Builder) -> Unit) {
        nativeClient.deleteWorkGroup(builder).await()
    }

    suspend fun getNamedQuery(builder: (GetNamedQueryRequest.Builder) -> Unit): NamedQuery {
        return nativeClient.getNamedQuery(builder).await().namedQuery()
    }

    suspend fun getQueryExecution(builder: (GetQueryExecutionRequest.Builder) -> Unit): QueryExecution {
        return nativeClient.getQueryExecution(builder).await().queryExecution()
    }

    suspend fun getUpdateCount(builder: (GetQueryResultsRequest.Builder) -> Unit): Long {
        return nativeClient.getQueryResults(builder).await().updateCount()
    }

    fun getQueryResults(builder: (GetQueryResultsRequest.Builder) -> Unit): Flow<List<Datum>> {
        return nativeClient.getQueryResultsPaginator(builder)
                .flatMapIterable { it.resultSet().rows() ?: emptyList() }
                .filter { it.hasData() }
                .map { it.data() }
                .asFlow()
    }

    suspend fun getWorkGroup(builder: (GetWorkGroupRequest.Builder) -> Unit): WorkGroup {
        return nativeClient.getWorkGroup(builder).await().workGroup()
    }

    fun listNamedQueries(builder: (ListNamedQueriesRequest.Builder) -> Unit): Flow<String> =
            nativeClient.listNamedQueriesPaginator(builder).flatMapIterable { it.namedQueryIds() ?: emptyList() }.asFlow()

    fun listQueryExecutions(builder: (ListQueryExecutionsRequest.Builder) -> Unit): Flow<String> =
            nativeClient.listQueryExecutionsPaginator(builder).flatMapIterable { it.queryExecutionIds() ?: emptyList() }.asFlow()

    suspend fun listTagsForResource(builder: (ListTagsForResourceRequest.Builder) -> Unit): Map<String, String> {
        val result = HashMap<String, String>()
        var nextToken: String? = null
        do {
            val rs = nativeClient.listTagsForResource() {
                it.nextToken(nextToken).maxResults(50).also(builder)
            }.await()
            rs.tags()?.forEach { result[it.key()] = it.value() }
            nextToken = rs.nextToken()
        } while (nextToken != null)
        return result
    }

    fun listWorkGroups(builder: (ListWorkGroupsRequest.Builder) -> Unit): Flow<WorkGroupSummary> =
            nativeClient.listWorkGroupsPaginator(builder).flatMapIterable { it.workGroups() ?: emptyList() }.asFlow()

    suspend fun startQueryExecution(builder: (StartQueryExecutionRequest.Builder) -> Unit): String {
        return nativeClient.startQueryExecution(builder).await().queryExecutionId()
    }

    suspend fun stopQueryExecution(builder: (StopQueryExecutionRequest.Builder) -> Unit) {
        nativeClient.stopQueryExecution(builder).await()
    }

    suspend fun tagResource(builder: (TagResourceRequest.Builder) -> Unit) {
        nativeClient.tagResource(builder).await()
    }

    suspend fun untagResource(builder: (UntagResourceRequest.Builder) -> Unit) {
        nativeClient.untagResource(builder).await()
    }

    suspend fun updateWorkGroup(builder: (UpdateWorkGroupRequest.Builder) -> Unit) {
        nativeClient.updateWorkGroup(builder).await()
    }
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, AthenaAsyncKlient>() }
fun SdkAsyncHttpClient.athena(region: Region,
                              builder: (AthenaAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            AthenaAsyncClient.builder().httpClient(this).region(region).also(builder).build()
                    .let { AthenaAsyncKlient(it) }
        }