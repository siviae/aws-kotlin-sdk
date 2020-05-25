@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.SnsAsyncClientBuilder
import software.amazon.awssdk.services.sns.model.*
import java.util.concurrent.Executor

class SnsAsyncKlient(val nativeClient: SnsAsyncClient) {
    suspend fun addPermission(builder: (AddPermissionRequest.Builder) -> Unit) {
        nativeClient.addPermission(builder).await()
    }

    suspend fun checkIfPhoneNumberIsOptedOut(builder: (CheckIfPhoneNumberIsOptedOutRequest.Builder) -> Unit): Boolean {
        return nativeClient.checkIfPhoneNumberIsOptedOut(builder).await().isOptedOut
    }

    suspend fun confirmSubscription(builder: (ConfirmSubscriptionRequest.Builder) -> Unit): String {
        return nativeClient.confirmSubscription(builder).await().subscriptionArn()
    }

    suspend fun createPlatformApplication(builder: (CreatePlatformApplicationRequest.Builder) -> Unit): String {
        return nativeClient.createPlatformApplication(builder).await().platformApplicationArn()
    }

    suspend fun createPlatformEndpoint(builder: (CreatePlatformEndpointRequest.Builder) -> Unit): String {
        return nativeClient.createPlatformEndpoint(builder).await().endpointArn()
    }

    suspend fun createTopic(builder: (CreateTopicRequest.Builder) -> Unit): String {
        return nativeClient.createTopic(builder).await().topicArn()
    }

    suspend fun deleteEndpoint(builder: (DeleteEndpointRequest.Builder) -> Unit) {
        nativeClient.deleteEndpoint(builder).await()
    }

    suspend fun deletePlatformApplication(builder: (DeletePlatformApplicationRequest.Builder) -> Unit) {
        nativeClient.deletePlatformApplication(builder).await()
    }

    suspend fun deleteTopic(builder: (DeleteTopicRequest.Builder) -> Unit) {
        nativeClient.deleteTopic(builder).await()
    }

    suspend fun getEndpointAttributes(builder: (GetEndpointAttributesRequest.Builder) -> Unit): Map<String, String> {
        return nativeClient.getEndpointAttributes(builder).await().attributes() ?: emptyMap()
    }

    suspend fun getPlatformApplicationAttributes(builder: (GetPlatformApplicationAttributesRequest.Builder) -> Unit): Map<String, String> {
        return nativeClient.getPlatformApplicationAttributes(builder).await().attributes() ?: emptyMap()
    }

    suspend fun getSMSAttributes(builder: (GetSmsAttributesRequest.Builder) -> Unit = {}): Map<String, String> {
        return nativeClient.getSMSAttributes(builder).await().attributes() ?: emptyMap()
    }

    suspend fun getSubscriptionAttributes(builder: (GetSubscriptionAttributesRequest.Builder) -> Unit): Map<String, String> {
        return nativeClient.getSubscriptionAttributes(builder).await().attributes() ?: emptyMap()
    }

    suspend fun getTopicAttributes(builder: (GetTopicAttributesRequest.Builder) -> Unit): Map<String, String> {
        return nativeClient.getTopicAttributes(builder).await().attributes() ?: emptyMap()
    }

    fun listEndpointsByPlatformApplication(builder: (ListEndpointsByPlatformApplicationRequest.Builder) -> Unit): Flow<Endpoint> =
            nativeClient.listEndpointsByPlatformApplicationPaginator(builder).endpoints().asFlow()

    fun listPhoneNumbersOptedOut(builder: (ListPhoneNumbersOptedOutRequest.Builder) -> Unit = {}): Flow<String> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listPhoneNumbersOptedOut {
                it.nextToken(nextToken).also(builder)
            }.await()
            result.phoneNumbers()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    fun listPlatformApplications(builder: (ListPlatformApplicationsRequest.Builder) -> Unit = {}): Flow<PlatformApplication> =
            nativeClient.listPlatformApplicationsPaginator(builder).platformApplications().asFlow()

    fun listSubscriptions(builder: (ListSubscriptionsRequest.Builder) -> Unit = {}): Flow<Subscription> =
            nativeClient.listSubscriptionsPaginator(builder).subscriptions().asFlow()

    fun listSubscriptionsByTopic(builder: (ListSubscriptionsByTopicRequest.Builder) -> Unit = {}): Flow<Subscription> =
            nativeClient.listSubscriptionsByTopicPaginator(builder).subscriptions().asFlow()

    suspend fun listTagsForResource(builder: (ListTagsForResourceRequest.Builder) -> Unit = {}): List<Tag> {
        return nativeClient.listTagsForResource(builder).await().tags() ?: emptyList()
    }

    fun listTopics(builder: (ListTopicsRequest.Builder) -> Unit = {}): Flow<Topic> =
            nativeClient.listTopicsPaginator(builder).topics().asFlow()

    suspend fun optInPhoneNumber(builder: (OptInPhoneNumberRequest.Builder) -> Unit) {
        nativeClient.optInPhoneNumber(builder).await()
    }

    suspend fun publish(builder: (PublishRequest.Builder) -> Unit): String {
        return nativeClient.publish(builder).await().messageId()
    }

    suspend fun removePermission(builder: (RemovePermissionRequest.Builder) -> Unit) {
        nativeClient.removePermission(builder).await()
    }

    suspend fun setEndpointAttributes(builder: (SetEndpointAttributesRequest.Builder) -> Unit) {
        nativeClient.setEndpointAttributes(builder).await()
    }

    suspend fun setPlatformApplicationAttributes(builder: (SetPlatformApplicationAttributesRequest.Builder) -> Unit) {
        nativeClient.setPlatformApplicationAttributes(builder).await()
    }

    suspend fun setSMSAttributes(builder: (SetSmsAttributesRequest.Builder) -> Unit) {
        nativeClient.setSMSAttributes(builder).await()
    }

    suspend fun setSubscriptionAttributes(builder: (SetSubscriptionAttributesRequest.Builder) -> Unit) {
        nativeClient.setSubscriptionAttributes(builder).await()
    }

    suspend fun setTopicAttributes(builder: (SetTopicAttributesRequest.Builder) -> Unit) {
        nativeClient.setTopicAttributes(builder).await()
    }

    suspend fun subscribe(builder: (SubscribeRequest.Builder) -> Unit): String {
        return nativeClient.subscribe(builder).await().subscriptionArn()
    }

    suspend fun tagResource(builder: (TagResourceRequest.Builder) -> Unit) {
        nativeClient.tagResource(builder).await()
    }

    suspend fun unsubscribe(builder: (UnsubscribeRequest.Builder) -> Unit) {
        nativeClient.unsubscribe(builder).await()
    }

    suspend fun untagResource(builder: (UntagResourceRequest.Builder) -> Unit) {
        nativeClient.untagResource(builder).await()
    }
}

fun SdkAsyncHttpClient.sns(region: Region,
                           builder: (SnsAsyncClientBuilder) -> Unit = {}) =
        SnsAsyncClient.builder()
                .httpClient(this)
                .region(region)
                .asyncConfiguration {
                    it.advancedOption(
                            SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR,
                            Executor { runnable -> runnable.run() }
                    )
                }
                .also(builder)
                .build()
                .let { SnsAsyncKlient(it) }