@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ec2.Ec2AsyncClient
import software.amazon.awssdk.services.ec2.Ec2AsyncClientBuilder
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest
import software.amazon.awssdk.services.ec2.model.DescribeVolumesRequest
import software.amazon.awssdk.services.ec2.model.Reservation
import software.amazon.awssdk.services.ec2.model.Volume
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
class Ec2AsyncKlient(val nativeClient: Ec2AsyncClient) {

    fun describeInstances(builder: (DescribeInstancesRequest.Builder) -> Unit = {}): Flow<Reservation> =
            nativeClient.describeInstancesPaginator(builder).reservations().asFlow()


    fun describeVolumes(builder: (DescribeVolumesRequest.Builder) -> Unit = {}): Flow<Volume> =
            nativeClient.describeVolumesPaginator(builder).volumes().asFlow()
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, Ec2AsyncKlient>() }

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.ec2(region: Region,
                           builder: (Ec2AsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            Ec2AsyncClient.builder().httpClient(this).region(region).also(builder).build()
                    .let { Ec2AsyncKlient(it) }
        }

