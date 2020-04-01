package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.await
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
suspend fun S3AsyncClient.listObjectsAsync(rq: (ListObjectsV2Request.Builder) -> Unit): Flow<S3Object> = flow {
    var nextToken: String? = null
    do {
        val result = listObjectsV2 { it.continuationToken(nextToken).maxKeys(5000).applyMutation(rq) }.await()
        result.contents().forEach { emit(it) }
        nextToken = result.nextContinuationToken()
    } while (nextToken != null)
}.flowOn(Dispatchers.IO)

@ExperimentalCoroutinesApi
suspend fun S3AsyncClient.deleteFileOrDirectoryAsync(bucket: String, path: String): Deferred<DeleteObjectResponse> {
    listObjectsAsync { it.bucket(bucket).prefix(path) }
            .map { obj -> deleteObject { (it.bucket(bucket)).key(obj.key()) }.asDeferred() }
            .buffer(100)
            .collect { it.await() }
    return deleteObject { it.bucket(bucket).key(path) }.asDeferred()
}

suspend fun S3AsyncClient.isObjectExists(rq: (HeadObjectRequest.Builder) -> Unit): Boolean {
    return try {
        headObject { rq(it) }.asDeferred().await()
        true
    } catch (e: NoSuchKeyException) {
        false
    }
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, S3AsyncClient>() }
fun SdkAsyncHttpClient.s3(region: Region) = clientByRegion.computeIfAbsent(region) {
    S3AsyncClient.builder().httpClient(this).region(it).build()
}