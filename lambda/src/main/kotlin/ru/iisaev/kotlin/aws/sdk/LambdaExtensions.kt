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
import software.amazon.awssdk.services.lambda.model.FunctionConfiguration
import software.amazon.awssdk.services.lambda.model.ListFunctionsRequest
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
suspend fun LambdaAsyncClient.listFunctionsAsync(rq: (ListFunctionsRequest.Builder) -> Unit): Flow<FunctionConfiguration> = flow {
    var nextToken: String? = null
    do {
        val result = listFunctions() { it.marker(nextToken).maxItems(5000).applyMutation(rq) }.await()
        result.functions().forEach { emit(it) }
        nextToken = result.nextMarker()
    } while (nextToken != null)
}.flowOn(Dispatchers.IO)

private val clientByRegion by lazy { ConcurrentHashMap<Region, LambdaAsyncClient>() }
fun SdkAsyncHttpClient.lambda(region: Region) = clientByRegion.computeIfAbsent(region) {
    LambdaAsyncClient.builder().httpClient(this).region(it).build()
}