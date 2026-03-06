package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.impl.slidingwindow.SlidingWindowRateLimiter
import ru.quipy.payments.metric.MetricBuilder
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
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
        .callTimeout(Duration.ofSeconds(35))
        .dispatcher(Dispatcher(Executors.newFixedThreadPool(parallelRequests)).apply {
            maxRequests = parallelRequests * 2
            maxRequestsPerHost = parallelRequests * 2
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

    private val coroutineDispatcher = Executors.newFixedThreadPool(parallelRequests).asCoroutineDispatcher()
    private val paymentScope = CoroutineScope(coroutineDispatcher + SupervisorJob())
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

        logger.debug(
            "[{}] Submitting payment request for payment {}, deadline: {}, now: {}",
            accountName,
            paymentId,
            deadline,
            now()
        )

        val currentTime = now()
        if (currentTime >= deadline) {
            logger.error("[{}] Payment {} deadline already passed at submission", accountName, paymentId)
            return CompletableFuture.completedFuture(false)
        }

        val transactionId = UUID.randomUUID()

        logger.info(
            "[{}] Submit: {} , txId: {}, timeLeft: {}ms",
            accountName,
            paymentId,
            transactionId,
            deadline - currentTime
        )

        return paymentScope.future {
            try {
                executeSinglePayment(transactionId, paymentId, amount, deadline)
            } catch (e: Exception) {
                logger.error("[{}] Payment processing failed for {}: {}", accountName, paymentId, e.message, e)
                false
            }
        }
    }

    private suspend fun executeSinglePayment(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
        deadline: Long
    ): Boolean {
        val currentTime = now()
        if (currentTime >= deadline) {
            logger.warn("[{}] Payment {}: deadline passed before execution", accountName, paymentId)
            return false
        }

        val timeLeft = deadline - currentTime
        val timeout = minOf(timeLeft, MAX_ATTEMPT_TIME)

        return try {
            withTimeout(timeout) {
                processPayment(transactionId, paymentId, amount)
            }
        } catch (e: TimeoutCancellationException) {
            logger.warn("[{}] Payment {} timed out after {}ms", accountName, paymentId, timeout)
            false
        } catch (e: Exception) {
            logger.error("[{}] Payment {} failed: {}", accountName, paymentId, e.message, e)
            false
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun processPayment(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int
    ): Boolean {
        incomingRegCounter.increment()
        val startTime = now()

        return try {
            semaphore.acquire()
            outgoingReqCounter.increment()

            rateLimiter.tick()

            val request = getPaymentRequest(transactionId, paymentId, amount)

            val response = suspendCancellableCoroutine<Response> { continuation ->
                val call = client.newCall(request)

                call.enqueue(object : Callback {
                    override fun onResponse(call: Call, response: Response) {
                        if (!continuation.isActive) {
                            response.close()
                            return
                        }
                        continuation.resume(response)
                    }

                    override fun onFailure(call: Call, e: java.io.IOException) {
                        if (!continuation.isActive) return
                        continuation.resumeWithException(e)
                    }
                })

                continuation.invokeOnCancellation {
                    if (!call.isCanceled()) {
                        call.cancel()
                    }
                }
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

                logger.debug(
                    "[{}] Payment processed for txId: {}, payment: {}, succeeded: {}",
                    accountName,
                    transactionId,
                    paymentId,
                    body.result
                )

                httpHandledRequestsTotalAccountCounter.increment()
                val processingTime = now() - startTime
                outgoingRequestProcessingTimeDistributionSummary.record(processingTime.toDouble())

                body.result
            }
        } catch (e: SocketTimeoutException) {
            retryCounter.increment()
            logger.error(
                "[{}] Payment timeout for txId: {}, payment: {}",
                accountName,
                transactionId,
                paymentId,
                e
            )
            throw e
        } catch (e: InterruptedIOException) {
            retryCounter.increment()
            logger.error(
                "[{}] Payment interrupted for txId: {}, payment: {}",
                accountName,
                transactionId,
                paymentId,
                e
            )
            throw e
        } catch (e: Exception) {
            retryCounter.increment()
            logger.error(
                "[{}] Payment failed for txId: {}, payment: {}",
                accountName,
                transactionId,
                paymentId,
                e
            )
            throw e
        } finally {
            semaphore.release()
            outgoingFinishedReqCounter.increment()
            incomingFinishedReqCounter.increment()
        }
    }

    private fun getPaymentRequest(transactionId: UUID, paymentId: UUID, amount: Int): Request {
        return Request.Builder().run {
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
        client.dispatcher.executorService.shutdown()
        (coroutineDispatcher.executor as? ExecutorService)?.shutdown()
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
