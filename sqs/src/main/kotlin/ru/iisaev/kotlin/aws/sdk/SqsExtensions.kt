@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.future.await
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder
import software.amazon.awssdk.services.sqs.model.*
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
class SqsAsyncKlient(val nativeClient: SqsAsyncClient) {

    suspend fun addPermission(builder: (AddPermissionRequest.Builder) -> Unit) {
        nativeClient.addPermission(builder).await()
    }

    suspend fun changeMessageVisibility(builder: (ChangeMessageVisibilityRequest.Builder) -> Unit) {
        nativeClient.changeMessageVisibility(builder).await()
    }

    suspend fun changeMessageVisibilityBatch(builder: (ChangeMessageVisibilityBatchRequest.Builder) -> Unit): ChangeMessageVisibilityBatchResponse {
        return nativeClient.changeMessageVisibilityBatch(builder).await()
    }

    suspend fun createQueue(builder: (CreateQueueRequest.Builder) -> Unit): String {
        return nativeClient.createQueue(builder).await().queueUrl()
    }

    suspend fun deleteMessage(builder: (DeleteMessageRequest.Builder) -> Unit) {
        nativeClient.deleteMessage(builder).await()
    }

    suspend fun deleteMessageBatch(builder: (DeleteMessageBatchRequest.Builder) -> Unit): DeleteMessageBatchResponse {
        return nativeClient.deleteMessageBatch(builder).await()
    }

    suspend fun deleteQueue(builder: (DeleteQueueRequest.Builder) -> Unit) {
        nativeClient.deleteQueue(builder).await()
    }

    suspend fun getQueueAttributes(builder: (GetQueueAttributesRequest.Builder) -> Unit): Map<String, String> {
        return nativeClient.getQueueAttributes(builder).await().attributesAsStrings() ?: emptyMap()
    }

    suspend fun getQueueUrl(builder: (GetQueueUrlRequest.Builder) -> Unit): String {
        return nativeClient.getQueueUrl(builder).await().queueUrl()
    }

    suspend fun listDeadLetterSourceQueues(builder: (ListDeadLetterSourceQueuesRequest.Builder) -> Unit): List<String> {
        return nativeClient.listDeadLetterSourceQueues(builder).await().queueUrls() ?: emptyList()
    }

    suspend fun listQueueTags(builder: (ListQueueTagsRequest.Builder) -> Unit): Map<String, String> {
        return nativeClient.listQueueTags(builder).await().tags() ?: emptyMap()
    }

    suspend fun listQueues(builder: (ListQueuesRequest.Builder) -> Unit = {}): List<String> {
        return nativeClient.listQueues(builder).await().queueUrls() ?: emptyList()
    }

    suspend fun purgeQueue(builder: (PurgeQueueRequest.Builder) -> Unit) {
        nativeClient.purgeQueue(builder).await()
    }

    suspend fun receiveMessage(builder: (ReceiveMessageRequest.Builder) -> Unit): List<Message> {
        return nativeClient.receiveMessage(builder).await().messages() ?: emptyList()
    }

    suspend fun removePermission(builder: (RemovePermissionRequest.Builder) -> Unit) {
        nativeClient.removePermission(builder).await()
    }

    suspend fun sendMessage(builder: (SendMessageRequest.Builder) -> Unit): SendMessageResponse {
        return nativeClient.sendMessage(builder).await()
    }

    suspend fun sendMessageBatch(builder: (SendMessageBatchRequest.Builder) -> Unit): SendMessageBatchResponse {
        return nativeClient.sendMessageBatch(builder).await()
    }

    suspend fun setQueueAttributes(builder: (SetQueueAttributesRequest.Builder) -> Unit) {
        nativeClient.setQueueAttributes(builder).await()
    }

    suspend fun tagQueue(builder: (TagQueueRequest.Builder) -> Unit) {
        nativeClient.tagQueue(builder).await()
    }

    suspend fun untagQueue(builder: (UntagQueueRequest.Builder) -> Unit) {
        nativeClient.untagQueue(builder).await()
    }
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, SqsAsyncKlient>() }

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.lambda(region: Region,
                              builder: (SqsAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            SqsAsyncClient.builder().httpClient(this).region(region).also(builder).build()
                    .let { SqsAsyncKlient(it) }
        }