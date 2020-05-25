@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.model.*
import java.nio.file.Path
import java.util.concurrent.Executor

@ExperimentalCoroutinesApi
class S3AsyncKlient(val nativeClient: S3AsyncClient) {
    suspend fun deleteFileOrDirectoryAsync(bucket: String, path: String): Deferred<DeleteObjectResponse> {
        listObjects { it.bucket(bucket).prefix(path) }
                .map { obj -> nativeClient.deleteObject { (it.bucket(bucket)).key(obj.key()) }.asDeferred() }
                .buffer(100)
                .collect { it.await() }
        return nativeClient.deleteObject { it.bucket(bucket).key(path) }.asDeferred()
    }

    suspend fun isObjectExists(rq: (HeadObjectRequest.Builder) -> Unit): Boolean {
        return try {
            nativeClient.headObject { rq(it) }.asDeferred().await()
            true
        } catch (e: NoSuchKeyException) {
            false
        }
    }

    suspend fun abortMultipartUpload(builder: (AbortMultipartUploadRequest.Builder) -> Unit): RequestCharged? {
        return nativeClient.abortMultipartUpload(builder).await().requestCharged()
    }

    suspend fun completeMultipartUpload(builder: (CompleteMultipartUploadRequest.Builder) -> Unit): CompleteMultipartUploadResponse {
        return nativeClient.completeMultipartUpload(builder).await()
    }

    suspend fun copyObject(builder: (CopyObjectRequest.Builder) -> Unit): CopyObjectResponse {
        return nativeClient.copyObject(builder).await()
    }

    suspend fun createBucket(builder: (CreateBucketRequest.Builder) -> Unit): String {
        return nativeClient.createBucket(builder).await().location()
    }

    suspend fun createMultipartUpload(builder: (CreateMultipartUploadRequest.Builder) -> Unit): CreateMultipartUploadResponse {
        return nativeClient.createMultipartUpload(builder).await()
    }

    suspend fun deleteBucket(builder: (DeleteBucketRequest.Builder) -> Unit) {
        nativeClient.deleteBucket(builder).await()
    }

    suspend fun deleteBucketAnalyticsConfiguration(builder: (DeleteBucketAnalyticsConfigurationRequest.Builder) -> Unit) {
        nativeClient.deleteBucketAnalyticsConfiguration(builder).await()
    }

    suspend fun deleteBucketCors(builder: (DeleteBucketCorsRequest.Builder) -> Unit) {
        nativeClient.deleteBucketCors(builder).await()
    }

    suspend fun deleteBucketEncryption(builder: (DeleteBucketEncryptionRequest.Builder) -> Unit) {
        nativeClient.deleteBucketEncryption(builder).await()
    }

    suspend fun deleteBucketInventoryConfiguration(builder: (DeleteBucketInventoryConfigurationRequest.Builder) -> Unit) {
        nativeClient.deleteBucketInventoryConfiguration(builder).await()
    }

    suspend fun deleteBucketLifecycle(builder: (DeleteBucketLifecycleRequest.Builder) -> Unit) {
        nativeClient.deleteBucketLifecycle(builder).await()
    }

    suspend fun deleteBucketMetricsConfiguration(builder: (DeleteBucketMetricsConfigurationRequest.Builder) -> Unit) {
        nativeClient.deleteBucketMetricsConfiguration(builder).await()
    }

    suspend fun deleteBucketPolicy(builder: (DeleteBucketPolicyRequest.Builder) -> Unit) {
        nativeClient.deleteBucketPolicy(builder).await()
    }

    suspend fun deleteBucketReplication(builder: (DeleteBucketReplicationRequest.Builder) -> Unit) {
        nativeClient.deleteBucketReplication(builder).await()
    }

    suspend fun deleteBucketTagging(builder: (DeleteBucketTaggingRequest.Builder) -> Unit) {
        nativeClient.deleteBucketTagging(builder).await()
    }

    suspend fun deleteBucketWebsite(builder: (DeleteBucketWebsiteRequest.Builder) -> Unit) {
        nativeClient.deleteBucketWebsite(builder).await()
    }

    suspend fun deleteObject(builder: (DeleteObjectRequest.Builder) -> Unit): DeleteObjectResponse {
        return nativeClient.deleteObject(builder).await()
    }

    suspend fun deleteObjectTagging(builder: (DeleteObjectTaggingRequest.Builder) -> Unit): String {
        return nativeClient.deleteObjectTagging(builder).await().versionId()
    }

    suspend fun deleteObjects(builder: (DeleteObjectsRequest.Builder) -> Unit): DeleteObjectsResponse {
        return nativeClient.deleteObjects(builder).await()
    }

    suspend fun deletePublicAccessBlock(builder: (DeletePublicAccessBlockRequest.Builder) -> Unit) {
        nativeClient.deletePublicAccessBlock(builder).await()
    }

    suspend fun getBucketAccelerateConfiguration(builder: (GetBucketAccelerateConfigurationRequest.Builder) -> Unit):
            GetBucketAccelerateConfigurationResponse {
        return nativeClient.getBucketAccelerateConfiguration(builder).await()
    }

    suspend fun getBucketAcl(builder: (GetBucketAclRequest.Builder) -> Unit): GetBucketAclResponse {
        return nativeClient.getBucketAcl(builder).await()
    }

    suspend fun getBucketAnalyticsConfiguration(builder: (GetBucketAnalyticsConfigurationRequest.Builder) -> Unit): AnalyticsConfiguration {
        return nativeClient.getBucketAnalyticsConfiguration(builder).await().analyticsConfiguration()
    }

    suspend fun getBucketCors(builder: (GetBucketCorsRequest.Builder) -> Unit): List<CORSRule> {
        return nativeClient.getBucketCors(builder).await().corsRules() ?: emptyList()
    }

    suspend fun getBucketEncryption(builder: (GetBucketEncryptionRequest.Builder) -> Unit): ServerSideEncryptionConfiguration {
        return nativeClient.getBucketEncryption(builder).await().serverSideEncryptionConfiguration()
    }

    suspend fun getBucketInventoryConfiguration(builder: (GetBucketInventoryConfigurationRequest.Builder) -> Unit): InventoryConfiguration {
        return nativeClient.getBucketInventoryConfiguration(builder).await().inventoryConfiguration()
    }

    suspend fun getBucketLifecycleConfiguration(builder: (GetBucketLifecycleConfigurationRequest.Builder) -> Unit): List<LifecycleRule> {
        return nativeClient.getBucketLifecycleConfiguration(builder).await().rules() ?: emptyList()
    }

    suspend fun getBucketLocation(builder: (GetBucketLocationRequest.Builder) -> Unit): Region {
        return nativeClient.getBucketLocation(builder).await().locationConstraintAsString().ifEmpty { null }?.let { Region.of(it) }
                ?: Region.US_EAST_1
    }

    suspend fun getBucketLogging(builder: (GetBucketLoggingRequest.Builder) -> Unit): LoggingEnabled {
        return nativeClient.getBucketLogging(builder).await().loggingEnabled()
    }

    suspend fun getBucketMetricsConfiguration(builder: (GetBucketMetricsConfigurationRequest.Builder) -> Unit): MetricsConfiguration {
        return nativeClient.getBucketMetricsConfiguration(builder).await().metricsConfiguration()
    }

    suspend fun getBucketNotificationConfiguration(builder: (GetBucketNotificationConfigurationRequest.Builder) -> Unit): GetBucketNotificationConfigurationResponse {
        return nativeClient.getBucketNotificationConfiguration(builder).await()
    }

    suspend fun getBucketPolicy(builder: (GetBucketPolicyRequest.Builder) -> Unit): String {
        return nativeClient.getBucketPolicy(builder).await().policy()
    }

    suspend fun getBucketPolicyStatus(builder: (GetBucketPolicyStatusRequest.Builder) -> Unit): PolicyStatus {
        return nativeClient.getBucketPolicyStatus(builder).await().policyStatus()
    }

    suspend fun getBucketReplication(builder: (GetBucketReplicationRequest.Builder) -> Unit): ReplicationConfiguration {
        return nativeClient.getBucketReplication(builder).await().replicationConfiguration()
    }

    suspend fun getBucketRequestPayment(builder: (GetBucketRequestPaymentRequest.Builder) -> Unit): Payer? {
        return nativeClient.getBucketRequestPayment(builder).await().payer()
    }

    suspend fun getBucketTagging(builder: (GetBucketTaggingRequest.Builder) -> Unit): Map<String, String> {
        return try {
            nativeClient.getBucketTagging(builder).await().tagSet()?.associate { it.key() to it.value() } ?: emptyMap()
        } catch (e: S3Exception) {
            if (e.awsErrorDetails().errorCode() == "NoSuchTagSet") {
                emptyMap()
            } else {
                throw e
            }
        }
    }

    suspend fun getBucketVersioning(builder: (GetBucketVersioningRequest.Builder) -> Unit): GetBucketVersioningResponse {
        return nativeClient.getBucketVersioning(builder).await()
    }

    suspend fun getBucketWebsite(builder: (GetBucketWebsiteRequest.Builder) -> Unit): GetBucketWebsiteResponse {
        return nativeClient.getBucketWebsite(builder).await()
    }

    suspend fun getObject(destinationPath: Path, builder: (GetObjectRequest.Builder) -> Unit): GetObjectResponse {
        return nativeClient.getObject(builder, destinationPath).await()
    }

    suspend fun getObjectAcl(builder: (GetObjectAclRequest.Builder) -> Unit): GetObjectAclResponse {
        return nativeClient.getObjectAcl(builder).await()
    }

    suspend fun getObjectLegalHold(builder: (GetObjectLegalHoldRequest.Builder) -> Unit): ObjectLockLegalHold {
        return nativeClient.getObjectLegalHold(builder).await().legalHold()
    }

    suspend fun getObjectLockConfiguration(builder: (GetObjectLockConfigurationRequest.Builder) -> Unit): ObjectLockConfiguration {
        return nativeClient.getObjectLockConfiguration(builder).await().objectLockConfiguration()
    }

    suspend fun getObjectRetention(builder: (GetObjectRetentionRequest.Builder) -> Unit): ObjectLockRetention {
        return nativeClient.getObjectRetention(builder).await().retention()
    }

    suspend fun getObjectTagging(builder: (GetObjectTaggingRequest.Builder) -> Unit): GetObjectTaggingResponse {
        return nativeClient.getObjectTagging(builder).await()
    }

    suspend fun getObjectTorrent(destinationPath: Path, builder: (GetObjectTorrentRequest.Builder) -> Unit): GetObjectTorrentResponse {
        return nativeClient.getObjectTorrent(builder, destinationPath).await()
    }

    suspend fun getPublicAccessBlock(builder: (GetPublicAccessBlockRequest.Builder) -> Unit): PublicAccessBlockConfiguration {
        return nativeClient.getPublicAccessBlock(builder).await().publicAccessBlockConfiguration()
    }

    suspend fun headBucket(builder: (HeadBucketRequest.Builder) -> Unit): HeadBucketResponse {
        return nativeClient.headBucket(builder).await()
    }

    suspend fun headObject(builder: (HeadObjectRequest.Builder) -> Unit): HeadObjectResponse {
        return nativeClient.headObject(builder).await()
    }

    fun listBucketAnalyticsConfigurations(builder: (ListBucketAnalyticsConfigurationsRequest.Builder) -> Unit): Flow<AnalyticsConfiguration> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listBucketAnalyticsConfigurations { it.continuationToken(nextToken).also(builder) }.await()
            result.analyticsConfigurationList()?.forEach { emit(it) }
            nextToken = result.nextContinuationToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    fun listBucketInventoryConfigurations(builder: (ListBucketInventoryConfigurationsRequest.Builder) -> Unit): Flow<InventoryConfiguration> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listBucketInventoryConfigurations { it.continuationToken(nextToken).also(builder) }.await()
            result.inventoryConfigurationList()?.forEach { emit(it) }
            nextToken = result.nextContinuationToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    fun listBucketMetricsConfigurations(builder: (ListBucketMetricsConfigurationsRequest.Builder) -> Unit): Flow<MetricsConfiguration> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listBucketMetricsConfigurations { it.continuationToken(nextToken).also(builder) }.await()
            result.metricsConfigurationList()?.forEach { emit(it) }
            nextToken = result.nextContinuationToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun listBuckets(builder: (ListBucketsRequest.Builder) -> Unit = {}): List<Bucket> {
        return nativeClient.listBuckets(builder).await().buckets() ?: emptyList()
    }

    fun listMultipartUploads(builder: (ListMultipartUploadsRequest.Builder) -> Unit): Flow<ListMultipartUploadsResponse> {
        return nativeClient.listMultipartUploadsPaginator(builder).asFlow()
    }

    fun listObjectVersions(builder: (ListObjectVersionsRequest.Builder) -> Unit): Flow<ListObjectVersionsResponse> {
        return nativeClient.listObjectVersionsPaginator(builder).asFlow()
    }

    fun listObjects(builder: (ListObjectsV2Request.Builder) -> Unit): Flow<S3Object> =
            nativeClient.listObjectsV2Paginator(builder).contents().asFlow()

    fun listParts(builder: (ListPartsRequest.Builder) -> Unit): Flow<ListPartsResponse> {
        return nativeClient.listPartsPaginator(builder).asFlow()
    }

    suspend fun putBucketAccelerateConfiguration(builder: (PutBucketAccelerateConfigurationRequest.Builder) -> Unit) {
        nativeClient.putBucketAccelerateConfiguration(builder).await()
    }

    suspend fun putBucketAcl(builder: (PutBucketAclRequest.Builder) -> Unit) {
        nativeClient.putBucketAcl(builder).await()
    }

    suspend fun putBucketAnalyticsConfiguration(builder: (PutBucketAnalyticsConfigurationRequest.Builder) -> Unit) {
        nativeClient.putBucketAnalyticsConfiguration(builder).await()
    }

    suspend fun putBucketCors(builder: (PutBucketCorsRequest.Builder) -> Unit) {
        nativeClient.putBucketCors(builder).await()
    }

    suspend fun putBucketEncryption(builder: (PutBucketEncryptionRequest.Builder) -> Unit) {
        nativeClient.putBucketEncryption(builder).await()
    }

    suspend fun putBucketInventoryConfiguration(builder: (PutBucketInventoryConfigurationRequest.Builder) -> Unit) {
        nativeClient.putBucketInventoryConfiguration(builder).await()
    }

    suspend fun putBucketLifecycleConfiguration(builder: (PutBucketLifecycleConfigurationRequest.Builder) -> Unit) {
        nativeClient.putBucketLifecycleConfiguration(builder).await()
    }

    suspend fun putBucketLogging(builder: (PutBucketLoggingRequest.Builder) -> Unit) {
        nativeClient.putBucketLogging(builder).await()
    }

    suspend fun putBucketMetricsConfiguration(builder: (PutBucketMetricsConfigurationRequest.Builder) -> Unit) {
        nativeClient.putBucketMetricsConfiguration(builder).await()
    }

    suspend fun putBucketNotificationConfiguration(builder: (PutBucketNotificationConfigurationRequest.Builder) -> Unit) {
        nativeClient.putBucketNotificationConfiguration(builder).await()
    }

    suspend fun putBucketPolicy(builder: (PutBucketPolicyRequest.Builder) -> Unit) {
        nativeClient.putBucketPolicy(builder).await()
    }

    suspend fun putBucketReplication(builder: (PutBucketReplicationRequest.Builder) -> Unit) {
        nativeClient.putBucketReplication(builder).await()
    }

    suspend fun putBucketRequestPayment(builder: (PutBucketRequestPaymentRequest.Builder) -> Unit) {
        nativeClient.putBucketRequestPayment(builder).await()
    }

    suspend fun putBucketTagging(builder: (PutBucketTaggingRequest.Builder) -> Unit) {
        nativeClient.putBucketTagging(builder).await()
    }

    suspend fun putBucketVersioning(builder: (PutBucketVersioningRequest.Builder) -> Unit) {
        nativeClient.putBucketVersioning(builder).await()
    }

    suspend fun putBucketWebsite(builder: (PutBucketWebsiteRequest.Builder) -> Unit) {
        nativeClient.putBucketWebsite(builder).await()
    }

    suspend fun putObject(body: AsyncRequestBody, builder: (PutObjectRequest.Builder) -> Unit): PutObjectResponse {
        return nativeClient.putObject(builder, body).await()
    }

    suspend fun putObjectAcl(builder: (PutObjectAclRequest.Builder) -> Unit): RequestCharged? {
        return nativeClient.putObjectAcl(builder).await().requestCharged()
    }

    suspend fun putObjectLegalHold(builder: (PutObjectLegalHoldRequest.Builder) -> Unit): RequestCharged? {
        return nativeClient.putObjectLegalHold(builder).await().requestCharged()
    }

    suspend fun putObjectLockConfiguration(builder: (PutObjectLockConfigurationRequest.Builder) -> Unit): RequestCharged? {
        return nativeClient.putObjectLockConfiguration(builder).await().requestCharged()
    }

    suspend fun putObjectRetention(builder: (PutObjectRetentionRequest.Builder) -> Unit): RequestCharged? {
        return nativeClient.putObjectRetention(builder).await().requestCharged()
    }

    suspend fun putObjectTagging(builder: (PutObjectTaggingRequest.Builder) -> Unit): String? {
        return nativeClient.putObjectTagging(builder).await().versionId()
    }

    suspend fun putPublicAccessBlock(builder: (PutPublicAccessBlockRequest.Builder) -> Unit) {
        nativeClient.putPublicAccessBlock(builder).await()
    }

    suspend fun restoreObject(builder: (RestoreObjectRequest.Builder) -> Unit): RestoreObjectResponse {
        return nativeClient.restoreObject(builder).await()
    }

    suspend fun uploadPart(body: AsyncRequestBody, builder: (UploadPartRequest.Builder) -> Unit): UploadPartResponse {
        return nativeClient.uploadPart(builder, body).await()
    }

    suspend fun uploadPartCopy(builder: (UploadPartCopyRequest.Builder) -> Unit): UploadPartCopyResponse {
        return nativeClient.uploadPartCopy(builder).await()
    }
}

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.s3(region: Region,
                          builder: (S3AsyncClientBuilder) -> Unit = {}) =
        S3AsyncClient.builder()
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
                .let { S3AsyncKlient(it) }