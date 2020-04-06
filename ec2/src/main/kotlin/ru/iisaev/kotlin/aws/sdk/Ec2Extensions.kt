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
import software.amazon.awssdk.services.ec2.Ec2AsyncClient
import software.amazon.awssdk.services.ec2.Ec2AsyncClientBuilder
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest
import software.amazon.awssdk.services.ec2.model.DescribeVolumesRequest
import software.amazon.awssdk.services.ec2.model.Reservation
import software.amazon.awssdk.services.ec2.model.Volume
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
class Ec2AsyncKlient(val nativeClient: Ec2AsyncClient) {

    suspend fun describeInstances(rq: (DescribeInstancesRequest.Builder) -> Unit): Flow<Reservation> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.describeInstances() { it.nextToken(nextToken).maxResults(5000).applyMutation(rq) }.await()
            result.reservations().forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)


    suspend fun describeVolumes(rq: (DescribeVolumesRequest.Builder) -> Unit): Flow<Volume> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.describeVolumes() { it.nextToken(nextToken).maxResults(5000).applyMutation(rq) }.await()
            result.volumes().forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, Ec2AsyncKlient>() }

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.ec2(region: Region,
                           builder: (Ec2AsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            Ec2AsyncClient.builder().httpClient(this).region(region).applyMutation(builder).build()
                    .let { Ec2AsyncKlient(it) }
        }

