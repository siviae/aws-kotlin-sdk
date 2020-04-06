@file:Suppress("MemberVisibilityCanBePrivate", "unused")
package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.athena.AthenaAsyncClient
import software.amazon.awssdk.services.athena.AthenaAsyncClientBuilder
import software.amazon.awssdk.services.athena.model.QueryExecutionState
import software.amazon.awssdk.services.athena.model.QueryExecutionStatus
import software.amazon.awssdk.services.athena.model.Row
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
        val maxThrottles: Int = Int.MAX_VALUE
)

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
suspend fun <T> String.runInAthena(config: AthenaConfig, mapper: (Row) -> T): Flow<T> = config.let { athena ->
    val executionId = throttle(config) { athena.client.nativeClient.startQueryExecution { it.workGroup(athena.workGroup).queryString(this) }.await()
            .queryExecutionId() }
    throttle(config) { waitForFinish(athena, executionId) }
    var nextToken: String? = null
    var last = false
    return flow {
        if (!last) {
            val results = throttle(config) {
                athena.client.nativeClient.getQueryResults {
                    it
                            .queryExecutionId(executionId)
                            .nextToken(nextToken)
                            .maxResults(athena.queryBatchSize)
                }.await()
            }
            nextToken = results.nextToken()
            last = (nextToken == null)
            results.resultSet().rows()
                    .forEach { emit(mapper(it)) }
        }
    }.flowOn(Dispatchers.IO)
}

@Throws(IllegalStateException::class)
suspend fun waitForFinish(config: AthenaConfig,
                          queryExecutionId: String) = config.let { athena ->
    var status: QueryExecutionStatus
    val delaySeq = generateSequence(athena.waitDelaySeed, athena.waitDelayFunction).iterator()
    do {
        val future = athena.client.nativeClient.getQueryExecution { it.queryExecutionId(queryExecutionId) }
        status = future.await().queryExecution().status()
        delay(delaySeq.next())
    } while (status.state() == QueryExecutionState.RUNNING || status.state() == QueryExecutionState.QUEUED)
    if (status.state() != QueryExecutionState.SUCCEEDED) {
        throw IllegalStateException("Invalid query status ${status.state()} due to ${status.stateChangeReason()}")
    }
}

class AthenaAsyncKlient(val nativeClient: AthenaAsyncClient) {

}

private val clientByRegion by lazy { ConcurrentHashMap<Region, AthenaAsyncKlient>() }
fun SdkAsyncHttpClient.athena(region: Region,
                              builder: (AthenaAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            AthenaAsyncClient.builder().httpClient(this).region(region).applyMutation(builder).build()
                    .let { AthenaAsyncKlient(it) }
        }