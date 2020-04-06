package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.future.await
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder
import software.amazon.awssdk.services.cloudwatch.model.*
import java.util.concurrent.ConcurrentHashMap

@ExperimentalCoroutinesApi
class CloudWatchAsyncKlient(val nativeClient: CloudWatchAsyncClient) {
    suspend fun deleteAlarms(builder: (DeleteAlarmsRequest.Builder) -> Unit) {
        nativeClient.deleteAlarms { builder(it) }.await()
    }

    suspend fun deleteAnomalyDetector(builder: (DeleteAnomalyDetectorRequest.Builder) -> Unit) {
        nativeClient.deleteAnomalyDetector { builder(it) }.await()
    }

    suspend fun deleteDashboards(builder: (DeleteDashboardsRequest.Builder) -> Unit) {
        nativeClient.deleteDashboards { builder(it) }.await()
    }

    suspend fun deleteInsightRules(builder: (DeleteInsightRulesRequest.Builder) -> Unit): List<PartialFailure> {
        return nativeClient.deleteInsightRules { builder(it) }.await().failures() ?: emptyList()
    }

    suspend fun describeAlarmHistory(batchSize: Int = 5000,
                                     builder: (DescribeAlarmHistoryRequest.Builder) -> Unit = {}): Flow<AlarmHistoryItem> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.describeAlarmHistory() {
                it.nextToken(nextToken).maxRecords(batchSize).applyMutation(builder)
            }.await()
            result.alarmHistoryItems()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun describeAlarms(batchSize: Int = 5000,
                               builder: (DescribeAlarmsRequest.Builder) -> Unit = {}): Flow<MetricAlarm> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.describeAlarms {
                it.nextToken(nextToken).maxRecords(batchSize).applyMutation(builder)
            }.await()
            result.metricAlarms()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun describeAlarmsForMetric(builder: (DescribeAlarmsForMetricRequest.Builder) -> Unit): List<MetricAlarm> {
        return nativeClient.describeAlarmsForMetric { builder(it) }.await().metricAlarms() ?: emptyList()
    }

    suspend fun describeAnomalyDetectors(batchSize: Int = 5000,
                                         builder: (DescribeAnomalyDetectorsRequest.Builder) -> Unit): Flow<AnomalyDetector> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.describeAnomalyDetectors {
                it.nextToken(nextToken).maxResults(batchSize).applyMutation(builder)
            }.await()
            result.anomalyDetectors()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun describeInsightRules(batchSize: Int = 5000,
                                     builder: (DescribeInsightRulesRequest.Builder) -> Unit = {}): Flow<InsightRule> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.describeInsightRules() {
                it.nextToken(nextToken).maxResults(batchSize).applyMutation(builder)
            }.await()
            result.insightRules()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun disableAlarmActions(builder: (DisableAlarmActionsRequest.Builder) -> Unit) {
        nativeClient.disableAlarmActions { builder(it) }.await()
    }

    suspend fun disableInsightRules(builder: (DisableInsightRulesRequest.Builder) -> Unit): List<PartialFailure> {
        return nativeClient.disableInsightRules { builder(it) }.await().failures() ?: emptyList()
    }

    suspend fun enableAlarmActions(builder: (EnableAlarmActionsRequest.Builder) -> Unit) {
        nativeClient.enableAlarmActions { builder(it) }.await()
    }

    suspend fun enableInsightRules(builder: (EnableInsightRulesRequest.Builder) -> Unit): List<PartialFailure> {
        return nativeClient.enableInsightRules { builder(it) }.await().failures() ?: emptyList()
    }

    suspend fun getDashboard(builder: (GetDashboardRequest.Builder) -> Unit): GetDashboardResponse {
        return nativeClient.getDashboard { builder(it) }.await()
    }

    suspend fun getInsightRuleReport(builder: (GetInsightRuleReportRequest.Builder) -> Unit): GetInsightRuleReportResponse {
        return nativeClient.getInsightRuleReport { builder(it) }.await()
    }

    suspend fun getMetricData(batchSize: Int = 5000,
                              builder: (GetMetricDataRequest.Builder) -> Unit): Flow<MetricDataResult> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.getMetricData() {
                it.nextToken(nextToken).maxDatapoints(batchSize).applyMutation(builder)
            }.await()
            result.metricDataResults()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun getMetricStatistics(builder: (GetMetricStatisticsRequest.Builder) -> Unit): GetMetricStatisticsResponse {
        return nativeClient.getMetricStatistics { builder(it) }.await()
    }

    suspend fun getMetricWidgetImage(builder: (GetMetricWidgetImageRequest.Builder) -> Unit): GetMetricWidgetImageResponse {
        return nativeClient.getMetricWidgetImage { builder(it) }.await()
    }

    suspend fun listDashboards(builder: (ListDashboardsRequest.Builder) -> Unit = {}): Flow<DashboardEntry> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listDashboards() { it.nextToken(nextToken).applyMutation(builder) }.await()
            result.dashboardEntries()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun listMetrics(builder: (ListMetricsRequest.Builder) -> Unit = {}): Flow<Metric> = flow {
        var nextToken: String? = null
        do {
            val result = nativeClient.listMetrics() { it.nextToken(nextToken).applyMutation(builder) }.await()
            result.metrics()?.forEach { emit(it) }
            nextToken = result.nextToken()
        } while (nextToken != null)
    }.flowOn(Dispatchers.IO)

    suspend fun listTagsForResource(builder: (ListTagsForResourceRequest.Builder) -> Unit): List<Tag> {
        return nativeClient.listTagsForResource { builder(it) }.await().tags() ?: emptyList()
    }

    suspend fun putAnomalyDetector(builder: (PutAnomalyDetectorRequest.Builder) -> Unit) {
        nativeClient.putAnomalyDetector { builder(it) }.await()
    }

    suspend fun putDashboard(builder: (PutDashboardRequest.Builder) -> Unit): List<DashboardValidationMessage> {
        return nativeClient.putDashboard { builder(it) }.await().dashboardValidationMessages() ?: emptyList()
    }

    suspend fun putInsightRule(builder: (PutInsightRuleRequest.Builder) -> Unit) {
        nativeClient.putInsightRule { builder(it) }.await()
    }

    suspend fun putMetricAlarm(builder: (PutMetricAlarmRequest.Builder) -> Unit) {
        nativeClient.putMetricAlarm { builder(it) }.await()
    }

    suspend fun putMetricData(builder: (PutMetricDataRequest.Builder) -> Unit) {
        nativeClient.putMetricData { builder(it) }.await()
    }

    suspend fun setAlarmState(builder: (SetAlarmStateRequest.Builder) -> Unit) {
        nativeClient.setAlarmState { builder(it) }.await()
    }

    suspend fun tagResource(builder: (TagResourceRequest.Builder) -> Unit) {
        nativeClient.tagResource { builder(it) }.await()
    }

    suspend fun untagResource(builder: (UntagResourceRequest.Builder) -> Unit) {
        nativeClient.untagResource { builder(it) }.await()
    }
}

private val clientByRegion by lazy { ConcurrentHashMap<Region, CloudWatchAsyncKlient>() }

@ExperimentalCoroutinesApi
fun SdkAsyncHttpClient.cloudWatch(region: Region,
                                  builder: (CloudWatchAsyncClientBuilder) -> Unit = {}) =
        clientByRegion.computeIfAbsent(region) {
            CloudWatchAsyncClient.builder().httpClient(this).region(region).applyMutation(builder).build()
                    .let { CloudWatchAsyncKlient(it) }
        }