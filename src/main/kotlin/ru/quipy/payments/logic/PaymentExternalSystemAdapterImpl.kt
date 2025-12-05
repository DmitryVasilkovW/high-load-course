package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.impl.slidingwindow.SlidingWindowRateLimiter
import ru.quipy.common.utils.retry.doRetry
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.metric.MetricBuilder
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    paymentProviderHostPort: String,
    token: String,
    metricBuilder: MetricBuilder,
) : PaymentExternalSystemAdapter, AutoCloseable {

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(10))
        .readTimeout(Duration.ofSeconds(30))
        .writeTimeout(Duration.ofSeconds(10))
        .callTimeout(Duration.ofSeconds(35))
        .dispatcher(Dispatcher().apply {
            maxRequests = 2000
            maxRequestsPerHost = 2000
        })
        .connectionPool(ConnectionPool(100, 5, TimeUnit.MINUTES))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val host = parseHost(paymentProviderHostPort)
    private val port = parsePort(paymentProviderHostPort)
    private val baseUrlComponents = mapOf(
        "serviceName" to serviceName,
        "token" to token,
        "accountName" to accountName,
    )

    private val paymentProcessingScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val dbOperationScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val rateLimiterScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val requestChannel = Channel<PaymentRequest>(capacity = Channel.UNLIMITED)
    private val activeRequests = AtomicLong(0)

    private val httpSemaphore = Semaphore(permits = parallelRequests)
    private val dbSemaphore = Semaphore(permits = 100)

    private val httpHandledRequestsTotalAccountCounter =
        metricBuilder.buildHttpHandledRequestsTotalCounter(properties.accountName)
    private val httpRequestsTotalAccountCounter =
        metricBuilder.buildHttpRequestsTotalCounter(properties.accountName)
    private val incomingRegCounter =
        metricBuilder.buildIncomingRegCounter(properties.accountName)
    private val incomingFinishedReqCounter =
        metricBuilder.buildIncomingFinishedReqCounter(properties.accountName)
    private val outgoingReqCounter =
        metricBuilder.buildOutgoingReqCounter(properties.accountName)
    private val outgoingFinishedReqCounter =
        metricBuilder.buildOutgoingFinishedReqCounter(properties.accountName)
    private val retryCounter =
        metricBuilder.buildRetryCounter(properties.accountName)
    private val outgoingRequestProcessingTimeDistributionSummary =
        metricBuilder.buildOutgoingRequestProcessingTimeDistributionSummary()

    init {
        startRequestProcessors()
        startQueueMonitor()
    }

    private fun startRequestProcessors() {
        repeat(parallelRequests) { processorId ->
            paymentProcessingScope.launch {
                requestChannel.consumeEach { request ->
                    try {
                        processPaymentRequest(request)
                    } catch (e: Exception) {
                        logger.error("[{}] Processor {} failed: {}", accountName, processorId, e.message, e)
                    }
                }
            }
        }
    }

    private fun startQueueMonitor() {
        paymentProcessingScope.launch {
            while (isActive) {
                delay(1000)
            }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Boolean> {
        httpRequestsTotalAccountCounter.increment()

        logger.info(
            "[{}] Submitting payment request for payment {}",
            accountName,
            paymentId,
        )

        val transactionId = UUID.randomUUID()
        val future = CompletableFuture<Boolean>()

        dbOperationScope.launch {
            try {
                dbSemaphore.withPermit {
                    paymentESService.update(paymentId) {
                        it.logSubmission(
                            success = true,
                            transactionId,
                            now(),
                            Duration.ofMillis(now() - paymentStartedAt)
                        )
                    }
                }
            } catch (e: Exception) {
                logger.error("[{}] Failed to log submission for payment {}", accountName, paymentId, e)
            }
        }

        val paymentRequest = PaymentRequest(
            transactionId = transactionId,
            paymentId = paymentId,
            amount = amount,
            startedAt = paymentStartedAt,
        )

        paymentProcessingScope.launch {
            try {
                requestChannel.send(paymentRequest)
            } catch (e: Exception) {
                logger.error("[{}] Failed to send payment to queue: {}", accountName, paymentId, e)
                future.completeExceptionally(e)
            }
        }

        return future
    }

    override fun name(): String {
        return accountName
    }

    override fun price(): Int {
        return properties.price
    }

    override fun isEnabled(): Boolean {
        return properties.enabled
    }

    override fun getProperties(): PaymentAccountProperties {
        return properties
    }

    override fun getRateLimit(): Long {
        return rateLimitPerSec.toLong()
    }

    override fun getProcessingTime(): Duration {
        return properties.averageProcessingTime
    }

    private suspend fun processPaymentRequest(request: PaymentRequest) {
        incomingRegCounter.increment()
        activeRequests.incrementAndGet()

        try {
            rateLimiterScope.launch {
                rateLimiter.tick()
            }.join()

            val result = doRetry(
                maxAttempts = 3,
                delay = properties.averageProcessingTime.toMillis(),
                retryOn = listOf(SocketTimeoutException::class, InterruptedIOException::class, Exception::class),
                recover = { handleRetryFailure(request) }
            ) {
                executePaymentRequest(request)
            }

            updatePaymentInDatabase(request, result as ExternalSysResponse)
        } finally {
            activeRequests.decrementAndGet()
            outgoingFinishedReqCounter.increment()
            incomingFinishedReqCounter.increment()
        }
    }

    private suspend fun executePaymentRequest(request: PaymentRequest): ExternalSysResponse {
        outgoingReqCounter.increment()
        val startTime = now()

        return try {
            httpSemaphore.withPermit {
                val httpRequest = getPaymentRequest(request)

                val response = withContext(Dispatchers.IO) {
                    client.newCall(httpRequest).execute()
                }

                response.use { resp ->
                    val body = try {
                        mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error(
                            "[{}] Failed to parse response for txId: {}, payment: {}",
                            accountName,
                            request.transactionId,
                            request.paymentId,
                            e
                        )
                        ExternalSysResponse(
                            request.transactionId.toString(),
                            request.paymentId.toString(),
                            false,
                            e.message
                        )
                    }

                    logger.info(
                        "[{}] Payment processed for txId: {}, payment: {}, succeeded: {}",
                        accountName,
                        request.transactionId,
                        request.paymentId,
                        body.result
                    )

                    httpHandledRequestsTotalAccountCounter.increment()
                    val processingTime = now() - startTime
                    outgoingRequestProcessingTimeDistributionSummary.record(processingTime.toDouble())

                    body
                }
            }
        } catch (e: Exception) {
            retryCounter.increment()
            throw e
        }
    }

    private fun handleRetryFailure(request: PaymentRequest) {
        dbOperationScope.launch {
            try {
                dbSemaphore.withPermit {
                    paymentESService.update(request.paymentId) {
                        it.logProcessing(
                            false,
                            now(),
                            request.transactionId,
                            reason = "All retry attempts failed"
                        )
                    }
                }
            } catch (e: Exception) {
                logger.error("[{}] Failed to log retry failure", accountName, e)
            }
        }
    }

    private suspend fun updatePaymentInDatabase(request: PaymentRequest, result: ExternalSysResponse) {
        try {
            dbSemaphore.withPermit {
                paymentESService.update(request.paymentId) {
                    it.logProcessing(
                        result.result,
                        now(),
                        request.transactionId,
                        reason = result.message
                    )
                }
            }
        } catch (e: Exception) {
            logger.error(
                "[{}] Failed to update payment in DB for txId: {}",
                accountName,
                request.transactionId,
                e
            )
        }
    }

    private fun getPaymentRequest(request: PaymentRequest): Request {
        return Request.Builder().run {
            val url = HttpUrl.Builder()
                .scheme("http")
                .host(host)
                .port(port)
                .addPathSegments("external/process")
                .apply {
                    baseUrlComponents.forEach { (key, value) -> addQueryParameter(key, value) }
                    addQueryParameter("transactionId", request.transactionId.toString())
                    addQueryParameter("paymentId", request.paymentId.toString())
                    addQueryParameter("amount", request.amount.toString())
                }
                .build()

            url(url).post(emptyBody)
        }.build()
    }

    private fun parseHost(hostPort: String): String {
        val parts = hostPort.split(":")
        return if (parts.size == 2) {
            parts[0]
        } else {
            hostPort
        }
    }

    private fun parsePort(hostPort: String): Int {
        val parts = hostPort.split(":")
        return if (parts.size == 2) {
            parts[1].toInt()
        } else {
            PORT
        }
    }

    override fun close() {
        paymentProcessingScope.cancel()
        dbOperationScope.cancel()
        rateLimiterScope.cancel()
        client.dispatcher.executorService.shutdown()
        requestChannel.close()
    }

    data class PaymentRequest(
        val transactionId: UUID,
        val paymentId: UUID,
        val amount: Int,
        val startedAt: Long
    )

    private fun now() = System.currentTimeMillis()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()

        private const val PORT = 80
    }
}
