@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder
import software.amazon.awssdk.services.dynamodb.model.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor

@ExperimentalCoroutinesApi
class DynamoDbAsyncKlient(val nativeClient: DynamoDbAsyncClient) {
    suspend fun batchGetItem(builder: (BatchGetItemRequest.Builder) -> Unit): BatchGetItemResponse {
        return nativeClient.batchGetItem(builder).await()
    }

    suspend fun batchWriteItem(builder: (BatchWriteItemRequest.Builder) -> Unit): BatchWriteItemResponse {
        return nativeClient.batchWriteItem(builder).await()
    }

    suspend fun createBackup(builder: (CreateBackupRequest.Builder) -> Unit): BackupDetails {
        return nativeClient.createBackup(builder).await().backupDetails()
    }

    suspend fun createGlobalTable(builder: (CreateGlobalTableRequest.Builder) -> Unit): GlobalTableDescription {
        return nativeClient.createGlobalTable(builder).await().globalTableDescription()
    }

    suspend fun createTable(builder: (CreateTableRequest.Builder) -> Unit): TableDescription {
        return nativeClient.createTable(builder).await().tableDescription()
    }

    suspend fun deleteBackup(builder: (DeleteBackupRequest.Builder) -> Unit): BackupDescription {
        return nativeClient.deleteBackup(builder).await().backupDescription()
    }

    suspend fun deleteItem(builder: (DeleteItemRequest.Builder) -> Unit): DeleteItemResponse {
        return nativeClient.deleteItem(builder).await()
    }

    suspend fun deleteTable(builder: (DeleteTableRequest.Builder) -> Unit): TableDescription {
        return nativeClient.deleteTable(builder).await().tableDescription()
    }

    suspend fun describeBackup(builder: (DescribeBackupRequest.Builder) -> Unit): BackupDescription {
        return nativeClient.describeBackup(builder).await().backupDescription()
    }

    suspend fun describeContinuousBackups(builder: (DescribeContinuousBackupsRequest.Builder) -> Unit): ContinuousBackupsDescription {
        return nativeClient.describeContinuousBackups(builder).await().continuousBackupsDescription()
    }

    suspend fun describeContributorInsights(builder: (DescribeContributorInsightsRequest.Builder) -> Unit): DescribeContributorInsightsResponse {
        return nativeClient.describeContributorInsights(builder).await()
    }

    suspend fun describeEndpoints(builder: (DescribeEndpointsRequest.Builder) -> Unit = {}): List<Endpoint> {
        return nativeClient.describeEndpoints(builder).await().endpoints() ?: emptyList()
    }

    suspend fun describeGlobalTable(builder: (DescribeGlobalTableRequest.Builder) -> Unit): GlobalTableDescription {
        return nativeClient.describeGlobalTable(builder).await().globalTableDescription()
    }

    suspend fun describeGlobalTableSettings(builder: (DescribeGlobalTableSettingsRequest.Builder) -> Unit): DescribeGlobalTableSettingsResponse {
        return nativeClient.describeGlobalTableSettings(builder).await()
    }

    suspend fun describeLimits(builder: (DescribeLimitsRequest.Builder) -> Unit = {}): DescribeLimitsResponse {
        return nativeClient.describeLimits(builder).await()
    }

    suspend fun describeTable(builder: (DescribeTableRequest.Builder) -> Unit): TableDescription {
        return nativeClient.describeTable(builder).await().table()
    }

    suspend fun describeTableReplicaAutoScaling(builder: (DescribeTableReplicaAutoScalingRequest.Builder) -> Unit): TableAutoScalingDescription {
        return nativeClient.describeTableReplicaAutoScaling(builder).await().tableAutoScalingDescription()
    }

    suspend fun describeTimeToLive(builder: (DescribeTimeToLiveRequest.Builder) -> Unit): TimeToLiveDescription {
        return nativeClient.describeTimeToLive(builder).await().timeToLiveDescription()
    }

    suspend fun getItem(builder: (GetItemRequest.Builder) -> Unit): Map<String, AttributeValue> {
        return nativeClient.getItem(builder).await().item()
    }

    fun listBackups(builder: (ListBackupsRequest.Builder) -> Unit = {}): Flow<BackupSummary> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listBackups { it.exclusiveStartBackupArn(nextToken).also(builder) }.await()
            result.backupSummaries()?.forEach { emit(it) }
            nextToken = result.lastEvaluatedBackupArn()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    fun listContributorInsights(builder: (ListContributorInsightsRequest.Builder) -> Unit): Flow<ContributorInsightsSummary> =
            nativeClient.listContributorInsightsPaginator(builder).flatMapIterable { it.contributorInsightsSummaries() ?: emptyList() }.asFlow()

    fun listGlobalTables(builder: (ListGlobalTablesRequest.Builder) -> Unit = {}): Flow<GlobalTable> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listGlobalTables { it.exclusiveStartGlobalTableName(nextToken).also(builder) }.await()
            result.globalTables()?.forEach { emit(it) }
            nextToken = result.lastEvaluatedGlobalTableName()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)


    fun listTables(builder: (ListTablesRequest.Builder) -> Unit = {}): Flow<String> =
            nativeClient.listTablesPaginator(builder).tableNames().asFlow()

    suspend fun listTagsOfResource(builder: (ListTagsOfResourceRequest.Builder) -> Unit): Map<String, String> {
        val result = HashMap<String, String>()
        var nextToken: String? = null
        do {
            val rs = nativeClient.listTagsOfResource() {
                it.nextToken(nextToken).also(builder)
            }.await()
            rs.tags()?.forEach { result[it.key()] = it.value() }
            nextToken = rs.nextToken()
        } while (nextToken != null)
        return result
    }

    suspend fun putItem(builder: (PutItemRequest.Builder) -> Unit): Map<String, AttributeValue> {
        return nativeClient.putItem(builder).await().attributes()
    }

    fun query(builder: (QueryRequest.Builder) -> Unit): Flow<Map<String, AttributeValue>> =
            nativeClient.queryPaginator(builder).items().asFlow()

    suspend fun restoreTableFromBackup(builder: (RestoreTableFromBackupRequest.Builder) -> Unit): TableDescription {
        return nativeClient.restoreTableFromBackup(builder).await().tableDescription()
    }

    suspend fun restoreTableToPointInTime(builder: (RestoreTableToPointInTimeRequest.Builder) -> Unit): TableDescription {
        return nativeClient.restoreTableToPointInTime(builder).await().tableDescription()
    }

    fun scan(builder: (ScanRequest.Builder) -> Unit): Flow<Map<String, AttributeValue>> =
            nativeClient.scanPaginator(builder).items().asFlow()

    suspend fun tagResource(builder: (TagResourceRequest.Builder) -> Unit) {
        nativeClient.tagResource(builder).await()
    }

    suspend fun transactGetItems(builder: (TransactGetItemsRequest.Builder) -> Unit): List<ItemResponse> {
        return nativeClient.transactGetItems(builder).await().responses() ?: emptyList()
    }

    suspend fun transactWriteItems(builder: (TransactWriteItemsRequest.Builder) -> Unit) {
        nativeClient.transactWriteItems(builder).await()
    }

    suspend fun untagResource(builder: (UntagResourceRequest.Builder) -> Unit) {
        nativeClient.untagResource(builder).await()
    }

    suspend fun updateContinuousBackups(builder: (UpdateContinuousBackupsRequest.Builder) -> Unit): ContinuousBackupsDescription {
        return nativeClient.updateContinuousBackups(builder).await().continuousBackupsDescription()
    }

    suspend fun updateContributorInsights(builder: (UpdateContributorInsightsRequest.Builder) -> Unit): UpdateContributorInsightsResponse {
        return nativeClient.updateContributorInsights(builder).await()
    }

    suspend fun updateGlobalTable(builder: (UpdateGlobalTableRequest.Builder) -> Unit): GlobalTableDescription {
        return nativeClient.updateGlobalTable(builder).await().globalTableDescription()
    }

    suspend fun updateGlobalTableSettings(builder: (UpdateGlobalTableSettingsRequest.Builder) -> Unit): UpdateGlobalTableSettingsResponse {
        return nativeClient.updateGlobalTableSettings(builder).await()
    }

    suspend fun updateItem(builder: (UpdateItemRequest.Builder) -> Unit): Map<String, AttributeValue> {
        return nativeClient.updateItem(builder).await().attributes()
    }

    suspend fun updateTable(builder: (UpdateTableRequest.Builder) -> Unit): TableDescription {
        return nativeClient.updateTable(builder).await().tableDescription()
    }

    suspend fun updateTableReplicaAutoScaling(builder: (UpdateTableReplicaAutoScalingRequest.Builder) -> Unit): TableAutoScalingDescription {
        return nativeClient.updateTableReplicaAutoScaling(builder).await().tableAutoScalingDescription()
    }

    suspend fun updateTimeToLive(builder: (UpdateTimeToLiveRequest.Builder) -> Unit): TimeToLiveSpecification {
        return nativeClient.updateTimeToLive(builder).await().timeToLiveSpecification()
    }
}

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.dynamoDb(region: Region,
                                builder: (DynamoDbAsyncClientBuilder) -> Unit = {}) =
        DynamoDbAsyncClient.builder()
                .httpClient(this)
                .region(region)
                .asyncConfiguration {
                    it.advancedOption(
                            SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                            Executor { runnable -> runnable.run() }
                    )
                }
                .also(builder)
                .build()
                .let { DynamoDbAsyncKlient(it) }