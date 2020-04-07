@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.lambda.LambdaAsyncClient
import software.amazon.awssdk.services.lambda.LambdaAsyncClientBuilder
import software.amazon.awssdk.services.lambda.model.FunctionConfiguration
import software.amazon.awssdk.services.lambda.model.ListFunctionsRequest
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
class LambdaAsyncKlient(val nativeClient: LambdaAsyncClient) {
    suspend fun listFunctions(batchSize: Int = 5000,
                              builder: (ListFunctionsRequest.Builder) -> Unit = {}): Flow<FunctionConfiguration> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listFunctions() { it.marker(nextToken).maxItems(batchSize).applyMutation(builder) }.await()
            result.functions().forEach { emit(it) }
            nextToken = result.nextMarker()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, LambdaAsyncKlient>() }

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.lambda(region: Region,
                              builder: (LambdaAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            LambdaAsyncClient.builder().httpClient(this).region(region).applyMutation(builder).build()
                    .let { LambdaAsyncKlient(it) }
        }