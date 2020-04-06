package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import java.util.concurrent.ConcurrentHashMap

private fun dynamoAttr(mutator: (AttributeValue.Builder) -> Unit) = AttributeValue.builder().applyMutation(mutator).build()!!

fun String.asDynamoAttr() = dynamoAttr { it.s(this) }
fun Enum<*>.asDynamoAttr() = this.name.asDynamoAttr()
fun Number.asDynamoAttr() = dynamoAttr { it.n(this.toString()) }

@JvmName("asDynamoEnumList")
fun Iterable<Enum<*>>.asDynamoAttr() = dynamoAttr { builder -> builder.ss(this.map { it.name }.toList()) }

@JvmName("asDynamoStringList")
fun Iterable<String>.asDynamoAttr() = dynamoAttr { builder -> builder.ss(this.map { it }.toList()) }

@JvmName("asDynamoNumberList")
fun Iterable<Number>.asDynamoAttr() = dynamoAttr { builder -> builder.ns(this.map { it.toString() }.toList()) }

@ExperimentalCoroutinesApi
class DynamoDbAsyncKlient(val nativeClient: DynamoDbAsyncClient) {
    fun query(rq: (QueryRequest.Builder) -> Unit): Flow<Map<String, AttributeValue>> = flow {
        var nextToken: Map<String, AttributeValue>? = null
        do {
            val result = nativeClient.query { it.exclusiveStartKey(nextToken).limit(1000).applyMutation(rq) }.await()
            result.items().forEach { emit(it) }
            nextToken = if (result.hasLastEvaluatedKey()) result.lastEvaluatedKey() else null
        } while (nextToken != null)
    }.flowOn(kotlinx.coroutines.Dispatchers.IO)


    fun scan(rq: (ScanRequest.Builder) -> Unit): Flow<Map<String, AttributeValue>> = flow {
        var nextToken: Map<String, AttributeValue>? = null
        do {
            val result = nativeClient.scan { it.exclusiveStartKey(nextToken).limit(1000).applyMutation(rq) }.await()
            result.items().forEach { emit(it) }
            nextToken = if (result.hasLastEvaluatedKey()) result.lastEvaluatedKey() else null
        } while (nextToken != null)
    }.flowOn(kotlinx.coroutines.Dispatchers.IO)
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, DynamoDbAsyncKlient>() }

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.dynamoDb(region: Region,
                                builder: (DynamoDbAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            DynamoDbAsyncClient.builder().httpClient(this).region(region).applyMutation(builder).build()
                    .let { DynamoDbAsyncKlient(it) }
        }