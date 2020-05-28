@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder
import software.amazon.awssdk.services.kinesis.model.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor

@ExperimentalCoroutinesApi
class KinesisAsyncKlient(val nativeClient: KinesisAsyncClient) {
    suspend fun addTagsToStream(builder: (AddTagsToStreamRequest.Builder) -> Unit) {
        nativeClient.addTagsToStream(builder).await()
    }

    suspend fun createStream(builder: (CreateStreamRequest.Builder) -> Unit) {
        nativeClient.createStream(builder).await()
    }

    suspend fun decreaseStreamRetentionPeriod(builder: (DecreaseStreamRetentionPeriodRequest.Builder) -> Unit) {
        nativeClient.decreaseStreamRetentionPeriod(builder).await()
    }

    suspend fun deleteStream(builder: (DeleteStreamRequest.Builder) -> Unit) {
        nativeClient.deleteStream(builder).await()
    }

    suspend fun deregisterStreamConsumer(builder: (DeregisterStreamConsumerRequest.Builder) -> Unit) {
        nativeClient.deregisterStreamConsumer(builder).await()
    }

    suspend fun describeLimits(builder: (DescribeLimitsRequest.Builder) -> Unit = {}): DescribeLimitsResponse {
        return nativeClient.describeLimits(builder).await()
    }

    suspend fun describeStream(builder: (DescribeStreamRequest.Builder) -> Unit): StreamDescription {
        return nativeClient.describeStream(builder).await().streamDescription()
    }

    suspend fun describeStreamConsumer(builder: (DescribeStreamConsumerRequest.Builder) -> Unit): ConsumerDescription {
        return nativeClient.describeStreamConsumer(builder).await().consumerDescription()
    }

    suspend fun describeStreamSummary(builder: (DescribeStreamSummaryRequest.Builder) -> Unit): StreamDescriptionSummary {
        return nativeClient.describeStreamSummary(builder).await().streamDescriptionSummary()
    }

    suspend fun disableEnhancedMonitoring(builder: (DisableEnhancedMonitoringRequest.Builder) -> Unit): DisableEnhancedMonitoringResponse {
        return nativeClient.disableEnhancedMonitoring(builder).await()
    }

    suspend fun enableEnhancedMonitoring(builder: (EnableEnhancedMonitoringRequest.Builder) -> Unit): EnableEnhancedMonitoringResponse {
        return nativeClient.enableEnhancedMonitoring(builder).await()
    }

    suspend fun getRecords(builder: (GetRecordsRequest.Builder) -> Unit): GetRecordsResponse {
        return nativeClient.getRecords(builder).await()
    }

    suspend fun getShardIterator(builder: (GetShardIteratorRequest.Builder) -> Unit): String {
        return nativeClient.getShardIterator(builder).await().shardIterator()
    }

    suspend fun increaseStreamRetentionPeriod(builder: (IncreaseStreamRetentionPeriodRequest.Builder) -> Unit) {
        nativeClient.increaseStreamRetentionPeriod(builder).await()
    }

    fun listShards(builder: (ListShardsRequest.Builder) -> Unit): Flow<Shard> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listShards { it.exclusiveStartShardId(nextToken).also(builder) }.await()
            result.shards()?.forEach { emit(it) }
            nextToken = result.shards()?.lastOrNull()?.shardId()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    fun listStreamConsumers(builder: (ListStreamConsumersRequest.Builder) -> Unit): Flow<Consumer> {
        return nativeClient.listStreamConsumersPaginator(builder).flatMapIterable { it.consumers() ?: emptyList() }.asFlow()
    }

    fun listStreams(builder: (ListStreamsRequest.Builder) -> Unit = {}): Flow<String> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listStreams { it.exclusiveStartStreamName(nextToken).also(builder) }.await()
            result.streamNames()?.forEach { emit(it) }
            nextToken = result.streamNames()?.lastOrNull()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun listTagsForStream(builder: (ListTagsForStreamRequest.Builder) -> Unit): Map<String, String> {
        return nativeClient.listTagsForStream(builder).await().tags()
                ?.map { it.key() to it.value() }
                ?.toMap()
                ?: emptyMap()
    }

    suspend fun mergeShards(builder: (MergeShardsRequest.Builder) -> Unit) {
        nativeClient.mergeShards(builder).await()
    }

    suspend fun putRecord(builder: (PutRecordRequest.Builder) -> Unit): PutRecordResponse {
        return nativeClient.putRecord(builder).await()
    }

    suspend fun putRecords(builder: (PutRecordsRequest.Builder) -> Unit): PutRecordsResponse {
        return nativeClient.putRecords(builder).await()
    }

    suspend fun registerStreamConsumer(builder: (RegisterStreamConsumerRequest.Builder) -> Unit): Consumer {
        return nativeClient.registerStreamConsumer(builder).await().consumer()
    }

    suspend fun removeTagsFromStream(builder: (RemoveTagsFromStreamRequest.Builder) -> Unit) {
        nativeClient.removeTagsFromStream(builder).await()
    }

    suspend fun splitShard(builder: (SplitShardRequest.Builder) -> Unit) {
        nativeClient.splitShard(builder).await()
    }

    suspend fun startStreamEncryption(builder: (StartStreamEncryptionRequest.Builder) -> Unit) {
        nativeClient.startStreamEncryption(builder).await()
    }

    suspend fun stopStreamEncryption(builder: (StopStreamEncryptionRequest.Builder) -> Unit) {
        nativeClient.stopStreamEncryption(builder).await()
    }

    //TODO implement
    /* fun subscribeToShard(builder: (SubscribeToShardRequest.Builder) -> Unit, asyncResponseHandler: SubscribeToShardResponseHandler?) {
         return nativeClient.subscribeToShard((SubscribeToShardRequest.builder().applyMutation(subscribeToShardRequest) as SubscribeToShardRequest.Builder).build() as SubscribeToShardRequest, asyncResponseHandler)
     }*/

    suspend fun updateShardCount(builder: (UpdateShardCountRequest.Builder) -> Unit): UpdateShardCountResponse {
        return nativeClient.updateShardCount(builder).await()
    }
}

@ExperimentalCoroutinesApi
private val clientCache = ConcurrentHashMap<Region, KinesisAsyncKlient>(Region.regions().size)

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.lambda(
        region: Region,
        builder: (KinesisAsyncClientBuilder) -> Unit = {}
) = clientCache.computeIfAbsent(region) {
    KinesisAsyncClient.builder()
            .httpClient(this)
            .region(region)
            .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
            .asyncConfiguration {
                it.advancedOption(
                        SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                        Executor { runnable -> runnable.run() }
                )
            }
            .also(builder)
            .build()
            .let { KinesisAsyncKlient(it) }
}