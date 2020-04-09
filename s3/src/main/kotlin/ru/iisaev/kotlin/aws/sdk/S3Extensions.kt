@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder
import software.amazon.awssdk.services.s3.model.*
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
class S3AsyncKlient(val nativeClient: S3AsyncClient) {
    suspend fun listObjects(builder: (ListObjectsV2Request.Builder) -> Unit): Flow<S3Object> =
            nativeClient.listObjectsV2Paginator(builder).contents().asFlow()


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
}


private val clientByRegion by lazy { ConcurrentHashMap<Region, S3AsyncKlient>() }

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.s3(region: Region,
                          builder: (S3AsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            S3AsyncClient.builder().httpClient(this).region(region).also(builder).build()
                    .let { S3AsyncKlient(it) }
        }