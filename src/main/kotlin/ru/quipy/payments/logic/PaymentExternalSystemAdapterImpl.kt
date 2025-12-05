package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
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
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.max

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
        .readTimeout(Duration.ofSeconds(30))
        .dispatcher( Dispatcher(Executors.newFixedThreadPool(10000)).apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectionPool(ConnectionPool(parallelRequests, 20, TimeUnit.SECONDS))
        .build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val host = parseHost(paymentProviderHostPort)
    private val port = parsePort(paymentProviderHostPort)
    private val baseUrlComponents = mapOf(
        "serviceName" to serviceName,
        "token" to token,
        "accountName" to accountName,
    )

    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val delay = max(requestAverageProcessingTime.toMillis(), 6000).toLong()

    private val paymentScope = CoroutineScope(Dispatchers.IO)
    private val semaphore = kotlinx.coroutines.sync.Semaphore(permits = parallelRequests)

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

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Boolean> {
        httpRequestsTotalAccountCounter.increment()

        logger.warn(
            "[{}] Submitting payment request for payment {}, deadline: {}, now: {}",
            accountName,
            paymentId,
            deadline,
            now()
        )

        val currentTime = now()
        if (currentTime >= deadline) {
            logger.error("[{}] Payment {} deadline already passed at submission", accountName, paymentId)
            val failedFuture = CompletableFuture<Boolean>()
            failedFuture.complete(false)
            return failedFuture
        }

        val transactionId = UUID.randomUUID()

        paymentScope.launch {
            try {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, currentTime, Duration.ofMillis(currentTime - paymentStartedAt))
                }
            } catch (e: Exception) {
                logger.error("[{}] Failed to log submission for payment {}", accountName, paymentId, e)
            }
        }

        logger.info(
            "[{}] Submit: {} , txId: {}, timeLeft: {}ms",
            accountName,
            paymentId,
            transactionId,
            deadline - currentTime
        )

        val future = CompletableFuture<Boolean>()

        paymentScope.launch {
            try {
                val result = handleExternalPaymentProcessingRequestWithDeadline(
                    transactionId = transactionId,
                    paymentId = paymentId,
                    amount = amount,
                    deadline = deadline
                )
                future.complete(result)
            } catch (e: Exception) {
                logger.error("[{}] Payment processing failed for {}: {}", accountName, paymentId, e.message)
                future.completeExceptionally(e)
            }
        }

        return future
    }

    private suspend fun handleExternalPaymentProcessingRequestWithDeadline(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
        deadline: Long
    ): Boolean {
        val currentTime = now()
        if (currentTime >= deadline) {
            logger.warn("[{}] Payment {} skipped: deadline passed before processing", accountName, paymentId)
            paymentScope.launch {
                try {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, currentTime, transactionId, reason = "Deadline passed before processing")
                    }
                } catch (e: Exception) {
                    logger.error("[{}] Failed to log deadline failure", accountName, e)
                }
            }
            return false
        }

        val timeLeftForAttempt = minOf(deadline - currentTime, MAX_ATTEMPT_TIME)

        return try {
            withTimeout(timeLeftForAttempt) {
                processWithResult(transactionId, paymentId, amount)
            }
        } catch (e: TimeoutCancellationException) {
            logger.warn("[{}] Payment {} timed out after {}ms", accountName, paymentId, timeLeftForAttempt)
            false
        } catch (e: Exception) {
            logger.error("[{}] Payment {} failed: {}", accountName, paymentId, e.message, e)
            false
        }
    }

    private suspend fun processWithResult(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int
    ): Boolean {
        incomingRegCounter.increment()
        val startTime = now()
        try {
            semaphore.acquire()
            val request = getPaymentRequest(transactionId, paymentId, amount)
            outgoingReqCounter.increment()

            rateLimiter.tick()

            val response = withContext(Dispatchers.IO) {
                client.newCall(request).execute()
            }

            response.use { resp ->
                val body = try {
                    mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error(
                        "[{}] Failed to parse response for txId: {}, payment: {}",
                        accountName,
                        transactionId,
                        paymentId,
                        e
                    )
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.info(
                    "[{}] Payment processed for txId: {}, payment: {}, succeeded: {}",
                    accountName,
                    transactionId,
                    paymentId,
                    body.result
                )

                paymentScope.launch {
                    try {
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    } catch (e: Exception) {
                        logger.error("[{}] Failed to update payment {} in DB", accountName, paymentId, e)
                    }
                }

                httpHandledRequestsTotalAccountCounter.increment()
                val processingTime = now() - startTime
                outgoingRequestProcessingTimeDistributionSummary.record(processingTime.toDouble())

                return body.result
            }
        } catch (e: SocketTimeoutException) {
            retryCounter.increment()
            logger.error(
                "[{}] Payment timeout for txId: {}, payment: {}",
                accountName,
                transactionId,
                paymentId,
                e,
            )
            throw e
        } catch (e: Exception) {
            retryCounter.increment()
            logger.error(
                "[{}] Payment failed for txId: {}, payment: {}",
                accountName,
                transactionId,
                paymentId,
                e,
            )
            throw e
        } finally {
            semaphore.release()
            outgoingFinishedReqCounter.increment()
            incomingFinishedReqCounter.increment()
        }
    }

    private fun getPaymentRequest(transactionId: UUID, paymentId: UUID, amount: Int): Request {
        val request = Request.Builder().run {
            val url = HttpUrl.Builder()
                .scheme("http")
                .host(host)
                .port(port)
                .addPathSegments("external/process")
                .apply {
                    baseUrlComponents.forEach { (key, value) -> addQueryParameter(key, value) }
                    addQueryParameter("transactionId", transactionId.toString())
                    addQueryParameter("paymentId", paymentId.toString())
                    addQueryParameter("amount", amount.toString())
                }
                .build()

            url(url).post(emptyBody)
        }.build()
        return request
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

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun getProperties(): PaymentAccountProperties = properties
    override fun getRateLimit(): Long = rateLimitPerSec.toLong()
    override fun getProcessingTime(): Duration = properties.averageProcessingTime
    override fun name() = properties.accountName

    override fun close() {
        paymentScope.cancel()
    }

    private fun now() = System.currentTimeMillis()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
        private const val PORT = 80
        private const val MAX_ATTEMPT_TIME = 30000L
    }
}