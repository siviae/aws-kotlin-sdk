@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.SnsAsyncClientBuilder
import java.util.concurrent.ConcurrentHashMap

class SnsAsyncKlient(val nativeClient: SnsAsyncClient)

private val clientByRegion by lazy { ConcurrentHashMap<Region, SnsAsyncKlient>() }
fun SdkAsyncHttpClient.ses(region: Region,
                           builder: (SnsAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            SnsAsyncClient.builder().httpClient(this).region(region).applyMutation(builder).build()
                    .let { SnsAsyncKlient(it) }
        }