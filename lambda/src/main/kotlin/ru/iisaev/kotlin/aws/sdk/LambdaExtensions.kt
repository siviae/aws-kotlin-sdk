@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.lambda.LambdaAsyncClient
import software.amazon.awssdk.services.lambda.LambdaAsyncClientBuilder
import software.amazon.awssdk.services.lambda.model.*
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
class LambdaAsyncKlient(val nativeClient: LambdaAsyncClient) {
    suspend fun addLayerVersionPermission(builder: (AddLayerVersionPermissionRequest.Builder) -> Unit): AddLayerVersionPermissionResponse {
        return nativeClient.addLayerVersionPermission(builder).await()
    }

    suspend fun addPermission(builder: (AddPermissionRequest.Builder) -> Unit): String {
        return nativeClient.addPermission(builder).await().statement()
    }

    suspend fun createAlias(builder: (CreateAliasRequest.Builder) -> Unit): CreateAliasResponse {
        return nativeClient.createAlias(builder).await()
    }

    suspend fun createEventSourceMapping(builder: (CreateEventSourceMappingRequest.Builder) -> Unit): CreateEventSourceMappingResponse {
        return nativeClient.createEventSourceMapping(builder).await()
    }

    suspend fun createFunction(builder: (CreateFunctionRequest.Builder) -> Unit): CreateFunctionResponse {
        return nativeClient.createFunction(builder).await()
    }

    suspend fun deleteAlias(builder: (DeleteAliasRequest.Builder) -> Unit) {
        nativeClient.deleteAlias(builder).await()
    }

    suspend fun deleteEventSourceMapping(builder: (DeleteEventSourceMappingRequest.Builder) -> Unit): DeleteEventSourceMappingResponse {
        return nativeClient.deleteEventSourceMapping(builder).await()
    }

    suspend fun deleteFunction(builder: (DeleteFunctionRequest.Builder) -> Unit) {
        nativeClient.deleteFunction(builder).await()
    }

    suspend fun deleteFunctionConcurrency(builder: (DeleteFunctionConcurrencyRequest.Builder) -> Unit) {
        nativeClient.deleteFunctionConcurrency(builder).await()
    }

    suspend fun deleteFunctionEventInvokeConfig(builder: (DeleteFunctionEventInvokeConfigRequest.Builder) -> Unit) {
        nativeClient.deleteFunctionEventInvokeConfig(builder).await()
    }

    suspend fun deleteLayerVersion(builder: (DeleteLayerVersionRequest.Builder) -> Unit) {
        nativeClient.deleteLayerVersion(builder).await()
    }

    suspend fun deleteProvisionedConcurrencyConfig(builder: (DeleteProvisionedConcurrencyConfigRequest.Builder) -> Unit) {
        nativeClient.deleteProvisionedConcurrencyConfig(builder).await()
    }

    suspend fun getAccountSettings(builder: (GetAccountSettingsRequest.Builder) -> Unit = {}): GetAccountSettingsResponse {
        return nativeClient.getAccountSettings(builder).await()
    }

    suspend fun getAlias(builder: (GetAliasRequest.Builder) -> Unit): GetAliasResponse {
        return nativeClient.getAlias(builder).await()
    }

    suspend fun getEventSourceMapping(builder: (GetEventSourceMappingRequest.Builder) -> Unit): GetEventSourceMappingResponse {
        return nativeClient.getEventSourceMapping(builder).await()
    }

    suspend fun getFunction(builder: (GetFunctionRequest.Builder) -> Unit): GetFunctionResponse {
        return nativeClient.getFunction(builder).await()
    }

    suspend fun getFunctionConcurrency(builder: (GetFunctionConcurrencyRequest.Builder) -> Unit): Int? {
        return nativeClient.getFunctionConcurrency(builder).await().reservedConcurrentExecutions()
    }

    suspend fun getFunctionConfiguration(builder: (GetFunctionConfigurationRequest.Builder) -> Unit): GetFunctionConfigurationResponse {
        return nativeClient.getFunctionConfiguration(builder).await()
    }

    suspend fun getFunctionEventInvokeConfig(builder: (GetFunctionEventInvokeConfigRequest.Builder) -> Unit): GetFunctionEventInvokeConfigResponse {
        return nativeClient.getFunctionEventInvokeConfig(builder).await()
    }

    suspend fun getLayerVersion(builder: (GetLayerVersionRequest.Builder) -> Unit): GetLayerVersionResponse {
        return nativeClient.getLayerVersion(builder).await()
    }

    suspend fun getLayerVersionByArn(builder: (GetLayerVersionByArnRequest.Builder) -> Unit): GetLayerVersionByArnResponse {
        return nativeClient.getLayerVersionByArn(builder).await()
    }

    suspend fun getLayerVersionPolicy(builder: (GetLayerVersionPolicyRequest.Builder) -> Unit): GetLayerVersionPolicyResponse {
        return nativeClient.getLayerVersionPolicy(builder).await()
    }

    suspend fun getPolicy(builder: (GetPolicyRequest.Builder) -> Unit): GetPolicyResponse {
        return nativeClient.getPolicy(builder).await()
    }

    suspend fun getProvisionedConcurrencyConfig(builder: (GetProvisionedConcurrencyConfigRequest.Builder) -> Unit): GetProvisionedConcurrencyConfigResponse {
        return nativeClient.getProvisionedConcurrencyConfig(builder).await()
    }

    suspend operator fun invoke(builder: (InvokeRequest.Builder) -> Unit): InvokeResponse {
        return nativeClient.invoke(builder).await()
    }

    fun listAliases(builder: (ListAliasesRequest.Builder) -> Unit): Flow<AliasConfiguration> {
        return nativeClient.listAliasesPaginator(builder).aliases().asFlow()
    }

    fun listEventSourceMappings(builder: (ListEventSourceMappingsRequest.Builder) -> Unit = {}):
            Flow<EventSourceMappingConfiguration> {
        return nativeClient.listEventSourceMappingsPaginator(builder).eventSourceMappings().asFlow()
    }

    fun listFunctionEventInvokeConfigs(builder: (ListFunctionEventInvokeConfigsRequest.Builder) -> Unit): Flow<FunctionEventInvokeConfig> {
        return nativeClient.listFunctionEventInvokeConfigsPaginator(builder).functionEventInvokeConfigs().asFlow()
    }

    fun listFunctions(builder: (ListFunctionsRequest.Builder) -> Unit = {}): Flow<FunctionConfiguration> =
            nativeClient.listFunctionsPaginator(builder).functions().asFlow()

    fun listLayerVersions(builder: (ListLayerVersionsRequest.Builder) -> Unit): Flow<LayerVersionsListItem> {
        return nativeClient.listLayerVersionsPaginator(builder).layerVersions().asFlow()
    }

    fun listLayers(builder: (ListLayersRequest.Builder) -> Unit = {}): Flow<LayersListItem> {
        return nativeClient.listLayersPaginator(builder).layers().asFlow()
    }

    fun listProvisionedConcurrencyConfigs(builder: (ListProvisionedConcurrencyConfigsRequest.Builder) -> Unit): Flow<ProvisionedConcurrencyConfigListItem> {
        return nativeClient.listProvisionedConcurrencyConfigsPaginator(builder).provisionedConcurrencyConfigs().asFlow()
    }

    suspend fun listTags(builder: (ListTagsRequest.Builder) -> Unit): Map<String, String> {
        return nativeClient.listTags(builder).await().tags() ?: emptyMap()
    }

    fun listVersionsByFunction(builder: (ListVersionsByFunctionRequest.Builder) -> Unit): Flow<FunctionConfiguration> {
        return nativeClient.listVersionsByFunctionPaginator(builder).versions().asFlow()
    }

    suspend fun publishLayerVersion(builder: (PublishLayerVersionRequest.Builder) -> Unit): PublishLayerVersionResponse {
        return nativeClient.publishLayerVersion(builder).await()
    }

    suspend fun publishVersion(builder: (PublishVersionRequest.Builder) -> Unit): PublishVersionResponse {
        return nativeClient.publishVersion(builder).await()
    }

    suspend fun putFunctionConcurrency(builder: (PutFunctionConcurrencyRequest.Builder) -> Unit): Int? {
        return nativeClient.putFunctionConcurrency(builder).await().reservedConcurrentExecutions()
    }

    suspend fun putFunctionEventInvokeConfig(builder: (PutFunctionEventInvokeConfigRequest.Builder) -> Unit): PutFunctionEventInvokeConfigResponse {
        return nativeClient.putFunctionEventInvokeConfig(builder).await()
    }

    suspend fun putProvisionedConcurrencyConfig(builder: (PutProvisionedConcurrencyConfigRequest.Builder) -> Unit): PutProvisionedConcurrencyConfigResponse {
        return nativeClient.putProvisionedConcurrencyConfig(builder).await()
    }

    suspend fun removeLayerVersionPermission(builder: (RemoveLayerVersionPermissionRequest.Builder) -> Unit) {
        nativeClient.removeLayerVersionPermission(builder).await()
    }

    suspend fun removePermission(builder: (RemovePermissionRequest.Builder) -> Unit) {
        nativeClient.removePermission(builder).await()
    }

    suspend fun tagResource(builder: (TagResourceRequest.Builder) -> Unit) {
        nativeClient.tagResource(builder).await()
    }

    suspend fun untagResource(builder: (UntagResourceRequest.Builder) -> Unit) {
        nativeClient.untagResource(builder).await()
    }

    suspend fun updateAlias(builder: (UpdateAliasRequest.Builder) -> Unit): UpdateAliasResponse {
        return nativeClient.updateAlias(builder).await()
    }

    suspend fun updateEventSourceMapping(builder: (UpdateEventSourceMappingRequest.Builder) -> Unit): UpdateEventSourceMappingResponse {
        return nativeClient.updateEventSourceMapping(builder).await()
    }

    suspend fun updateFunctionCode(builder: (UpdateFunctionCodeRequest.Builder) -> Unit): UpdateFunctionCodeResponse {
        return nativeClient.updateFunctionCode(builder).await()
    }

    suspend fun updateFunctionConfiguration(builder: (UpdateFunctionConfigurationRequest.Builder) -> Unit): UpdateFunctionConfigurationResponse {
        return nativeClient.updateFunctionConfiguration(builder).await()
    }

    suspend fun updateFunctionEventInvokeConfig(builder: (UpdateFunctionEventInvokeConfigRequest.Builder) -> Unit): UpdateFunctionEventInvokeConfigResponse {
        return nativeClient.updateFunctionEventInvokeConfig(builder).await()
    }
}

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.lambda(region: Region,
                              builder: (LambdaAsyncClientBuilder) -> Unit = {}) =
        LambdaAsyncClient.builder().httpClient(this).region(region).also(builder).build()
                .let { LambdaAsyncKlient(it) }