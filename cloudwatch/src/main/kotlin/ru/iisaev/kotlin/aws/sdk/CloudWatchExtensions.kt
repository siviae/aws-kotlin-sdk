@file:Suppress("MemberVisibilityCanBePrivate", "unused")

package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.cloudwatch.model.*
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor

@ExperimentalCoroutinesApi
class CloudWatchAsyncKlient(val nativeClient: CloudWatchAsyncClient) {
    suspend fun deleteAlarms(builder: (DeleteAlarmsRequest.Builder) -> Unit) {
        nativeClient.deleteAlarms(builder).await()
    }

    suspend fun deleteAnomalyDetector(builder: (DeleteAnomalyDetectorRequest.Builder) -> Unit) {
        nativeClient.deleteAnomalyDetector(builder).await()
    }

    suspend fun deleteDashboards(builder: (DeleteDashboardsRequest.Builder) -> Unit) {
        nativeClient.deleteDashboards(builder).await()
    }

    suspend fun deleteInsightRules(builder: (DeleteInsightRulesRequest.Builder) -> Unit): List<PartialFailure> {
        return nativeClient.deleteInsightRules(builder).await().failures() ?: emptyList()
    }

    fun describeAlarmHistory(builder: (DescribeAlarmHistoryRequest.Builder) -> Unit = {}): Flow<AlarmHistoryItem> =
            nativeClient.describeAlarmHistoryPaginator(builder).alarmHistoryItems().asFlow()

    fun describeAlarms(builder: (DescribeAlarmsRequest.Builder) -> Unit = {}): Flow<MetricAlarm> =
            nativeClient.describeAlarmsPaginator(builder).metricAlarms().asFlow()

    suspend fun describeAlarmsForMetric(builder: (DescribeAlarmsForMetricRequest.Builder) -> Unit): List<MetricAlarm> {
        return nativeClient.describeAlarmsForMetric(builder).await().metricAlarms() ?: emptyList()
    }

    fun describeAnomalyDetectors(builder: (DescribeAnomalyDetectorsRequest.Builder) -> Unit): Flow<AnomalyDetector> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.describeAnomalyDetectors {
                it.nextToken(nextToken).maxResults(10).also(builder)
            }.await()
            result.anomalyDetectors()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    fun describeInsightRules(builder: (DescribeInsightRulesRequest.Builder) -> Unit = {}): Flow<InsightRule> =
            nativeClient.describeInsightRulesPaginator(builder).flatMapIterable { it.insightRules() ?: emptyList() }.asFlow()

    suspend fun disableAlarmActions(builder: (DisableAlarmActionsRequest.Builder) -> Unit) {
        nativeClient.disableAlarmActions(builder).await()
    }

    suspend fun disableInsightRules(builder: (DisableInsightRulesRequest.Builder) -> Unit): List<PartialFailure> {
        return nativeClient.disableInsightRules(builder).await().failures() ?: emptyList()
    }

    suspend fun enableAlarmActions(builder: (EnableAlarmActionsRequest.Builder) -> Unit) {
        nativeClient.enableAlarmActions(builder).await()
    }

    suspend fun enableInsightRules(builder: (EnableInsightRulesRequest.Builder) -> Unit): List<PartialFailure> {
        return nativeClient.enableInsightRules(builder).await().failures() ?: emptyList()
    }

    suspend fun getDashboard(builder: (GetDashboardRequest.Builder) -> Unit): GetDashboardResponse {
        return nativeClient.getDashboard(builder).await()
    }

    suspend fun getInsightRuleReport(builder: (GetInsightRuleReportRequest.Builder) -> Unit): GetInsightRuleReportResponse {
        return nativeClient.getInsightRuleReport(builder).await()
    }

    fun getMetricData(builder: (GetMetricDataRequest.Builder) -> Unit): Flow<MetricDataResult> =
            nativeClient.getMetricDataPaginator(builder).metricDataResults().asFlow()

    suspend fun getMetricStatistics(builder: (GetMetricStatisticsRequest.Builder) -> Unit): GetMetricStatisticsResponse {
        return nativeClient.getMetricStatistics(builder).await()
    }

    suspend fun getMetricWidgetImage(builder: (GetMetricWidgetImageRequest.Builder) -> Unit): ByteBuffer {
        return nativeClient.getMetricWidgetImage(builder).await().metricWidgetImage().asByteBuffer()
    }

    fun listDashboards(builder: (ListDashboardsRequest.Builder) -> Unit = {}): Flow<DashboardEntry> =
            nativeClient.listDashboardsPaginator(builder).dashboardEntries().asFlow()

    fun listMetrics(builder: (ListMetricsRequest.Builder) -> Unit = {}): Flow<Metric> =
            nativeClient.listMetricsPaginator(builder).metrics().asFlow()

    suspend fun listTagsForResource(builder: (ListTagsForResourceRequest.Builder) -> Unit): List<Tag> {
        return nativeClient.listTagsForResource(builder).await().tags() ?: emptyList()
    }

    suspend fun putAnomalyDetector(builder: (PutAnomalyDetectorRequest.Builder) -> Unit) {
        nativeClient.putAnomalyDetector(builder).await()
    }

    suspend fun putDashboard(builder: (PutDashboardRequest.Builder) -> Unit): List<DashboardValidationMessage> {
        return nativeClient.putDashboard(builder).await().dashboardValidationMessages() ?: emptyList()
    }

    suspend fun putInsightRule(builder: (PutInsightRuleRequest.Builder) -> Unit) {
        nativeClient.putInsightRule(builder).await()
    }

    suspend fun putMetricAlarm(builder: (PutMetricAlarmRequest.Builder) -> Unit) {
        nativeClient.putMetricAlarm(builder).await()
    }

    suspend fun putMetricData(builder: (PutMetricDataRequest.Builder) -> Unit) {
        nativeClient.putMetricData(builder).await()
    }

    suspend fun setAlarmState(builder: (SetAlarmStateRequest.Builder) -> Unit) {
        nativeClient.setAlarmState(builder).await()
    }

    suspend fun tagResource(builder: (TagResourceRequest.Builder) -> Unit) {
        nativeClient.tagResource(builder).await()
    }

    suspend fun untagResource(builder: (UntagResourceRequest.Builder) -> Unit) {
        nativeClient.untagResource(builder).await()
    }
}

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.cloudWatch(region: Region,
                                  builder: (CloudWatchAsyncClientBuilder) -> Unit = {}) =
        CloudWatchAsyncClient.builder()
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
                .let { CloudWatchAsyncKlient(it) }