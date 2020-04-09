@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.lambda.LambdaAsyncClient
import software.amazon.awssdk.services.lambda.LambdaAsyncClientBuilder
import software.amazon.awssdk.services.lambda.model.FunctionConfiguration
import software.amazon.awssdk.services.lambda.model.ListFunctionsRequest
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
class LambdaAsyncKlient(val nativeClient: LambdaAsyncClient) {
    fun listFunctions(builder: (ListFunctionsRequest.Builder) -> Unit = {}): Flow<FunctionConfiguration> =
            nativeClient.listFunctionsPaginator(builder).functions().asFlow()
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, LambdaAsyncKlient>() }

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.lambda(region: Region,
                              builder: (LambdaAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            LambdaAsyncClient.builder().httpClient(this).region(region).also(builder).build()
                    .let { LambdaAsyncKlient(it) }
        }