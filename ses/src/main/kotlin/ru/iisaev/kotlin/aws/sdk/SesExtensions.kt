package ru.iisaev.kotlin.aws.sdk

import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ses.SesAsyncClient
import java.util.concurrent.ConcurrentHashMap


private val clientByRegion by lazy { ConcurrentHashMap<Region, SesAsyncClient>() }
fun SdkAsyncHttpClient.ses(region: Region) = clientByRegion.computeIfAbsent(region) {
    SesAsyncClient.builder().httpClient(this).region(it).build()
}