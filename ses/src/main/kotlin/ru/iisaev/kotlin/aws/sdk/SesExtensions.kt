package ru.iisaev.kotlin.aws.sdk

import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ses.SesAsyncClient
import software.amazon.awssdk.services.ses.SesAsyncClientBuilder
import java.util.concurrent.ConcurrentHashMap

class SesAsyncKlient(val nativeClient: SesAsyncClient)

private val clientByRegion by lazy { ConcurrentHashMap<Region, SesAsyncKlient>() }
fun SdkAsyncHttpClient.ses(region: Region,
                           builder: (SesAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            SesAsyncClient.builder().httpClient(this).region(region).applyMutation(builder).build()
                    .let { SesAsyncKlient(it) }
        }