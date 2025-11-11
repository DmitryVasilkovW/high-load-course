package ru.quipy.payments.metric

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Service

@Service
class MetricBuilder(private val meterRegistry: MeterRegistry) {

    fun buildHttpHandledRequestsTotalCounter(accountName: String = ALL): Counter = Counter
        .builder(HTTP_HANDLED_REQUESTS_TOTAL)
        .description(TOTAL_NUMBER_OF_HANDLED_HTTP_REQUESTS)
        .tag(ACC, accountName)
        .register(meterRegistry)

    fun buildHttpRequestsTotalCounter(accountName: String = ALL): Counter = Counter
        .builder(HTTP_REQUESTS_TOTAL)
        .description(TOTAL_NUMBER_OF_HTTP_REQUESTS)
        .tag(ACC, accountName)
        .register(meterRegistry)

    fun buildIncomingRegCounter(accountName: String = ALL): Counter = Counter
        .builder(INCOMING_STARTED)
        .description(INCOMING_STARTED_REQUEST)
        .tag(ACC, accountName)
        .register(meterRegistry)

    fun buildIncomingFinishedReqCounter(accountName: String = ALL): Counter = Counter
        .builder(INCOMING_FINISHED)
        .description(INCOMING_FINISHED_REQUEST)
        .tag(ACC, accountName)
        .register(meterRegistry)

    fun buildOutgoingReqCounter(accountName: String = ALL): Counter = Counter
        .builder(OUTGOING_STARTED)
        .description(OUTGOING_STARTED_REQUEST)
        .tag(ACC, accountName)
        .register(meterRegistry)

    fun buildOutgoingFinishedReqCounter(accountName: String = ALL): Counter = Counter
        .builder(OUTGOING_FINISHED)
        .description(OUTGOING_FINISHED_REQUEST)
        .tag(ACC, accountName)
        .register(meterRegistry)

    fun buildRetryCounter(accountName: String = ALL): Counter = Counter
        .builder(OUTGOING_REQUEST_RETRIES)
        .description(NUMBER_OF_RETRIES_FOR_OUTGOING_REQUESTS)
        .tag(ACC, accountName)
        .register(meterRegistry)

    fun buildOutgoingRequestProcessingTimeDistributionSummary(accountName: String = ALL) = Timer
        .builder(OUTGOING_REQUEST_PROCESSING_TIME)
        .description(OUTGOING_REQUEST_LATENCY)
        .tag(ACC, accountName)
        .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99)
        .register(meterRegistry)

    companion object {
        private const val ACC = "acc"
        private const val ALL = "all"

        private const val HTTP_HANDLED_REQUESTS_TOTAL = "http_handled_requests_total"
        private const val TOTAL_NUMBER_OF_HANDLED_HTTP_REQUESTS = "Total number of handled HTTP requests"
        private const val HTTP_REQUESTS_TOTAL = "http_requests_total"
        private const val TOTAL_NUMBER_OF_HTTP_REQUESTS = "Total number of HTTP requests"
        private const val INCOMING_STARTED = "incoming_started_total"
        private const val INCOMING_STARTED_REQUEST = "incoming started request"
        private const val INCOMING_FINISHED = "incoming_finished_total"
        private const val INCOMING_FINISHED_REQUEST = "incoming finished request"
        private const val OUTGOING_STARTED = "outgoing_started_total"
        private const val OUTGOING_STARTED_REQUEST = "outgoing started request"
        private const val OUTGOING_FINISHED = "outgoing_finished_total"
        private const val OUTGOING_FINISHED_REQUEST = "outgoing finished request"
        private const val OUTGOING_REQUEST_RETRIES = "outgoing_request_retries_total"
        private const val NUMBER_OF_RETRIES_FOR_OUTGOING_REQUESTS = "Number of retries for outgoing requests"
        private const val OUTGOING_REQUEST_PROCESSING_TIME = "outgoing_request_processing_time_sum"
        private const val OUTGOING_REQUEST_LATENCY = "Outgoing request latency"
    }
}
