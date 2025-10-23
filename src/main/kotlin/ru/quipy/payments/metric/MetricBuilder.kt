package ru.quipy.payments.metric

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Counter
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

    companion object {
        private const val ACC = "acc"
        private const val ALL = "all"

        private const val HTTP_HANDLED_REQUESTS_TOTAL = "http_handled_requests_total"
        private const val TOTAL_NUMBER_OF_HANDLED_HTTP_REQUESTS = "Total number of handled HTTP requests"
        private const val HTTP_REQUESTS_TOTAL = "http_requests_total"
        private const val TOTAL_NUMBER_OF_HTTP_REQUESTS = "Total number of HTTP requests"
    }
}
