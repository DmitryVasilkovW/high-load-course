package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import kotlinx.coroutines.sync.Semaphore
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.impl.slidingwindow.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.metric.MetricBuilder
import java.io.IOException
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
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

    private val rejectedCountingPolicy = RejectedExecutionHandler { r, executor ->
        rejectedTasksCounter.increment()
        if (!executor.isShutdown) {
            r.run()
        }
    }

    private val paymentExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(parallelRequests * 2),
        NamedThreadFactory("payment-submission-executor"),
        rejectedCountingPolicy
    ).apply {
        allowCoreThreadTimeOut(false)
    }

    private val okHttpExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(parallelRequests * 2),
        NamedThreadFactory("okhttp-dispatcher-executor"),
        rejectedCountingPolicy
    )

    private val scheduler = Executors.newScheduledThreadPool(
        1,
        NamedThreadFactory("hedge-scheduler-${accountName}")
    )

    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofMillis(1000L))
        .dispatcher(Dispatcher(okHttpExecutor).apply {
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
    private val semaphore = Semaphore(permits = parallelRequests)

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
    private val rejectedTasksCounter = metricBuilder.buildRejectCounter(properties.accountName)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val future = CompletableFuture<Boolean>()
        val completed = AtomicBoolean(false)
        var scheduledFuture: ScheduledFuture<*>? = null

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val startTime = now()
        val timeRemaining = deadline - startTime
        if (timeRemaining <= 0) {
            future.complete(false)
            
            return future
        }

        val attemptsLeft = AtomicInteger(1)

        fun sendAttempt() {
            if (semaphore.tryAcquire().not()) {
                Thread.currentThread().interrupt()
                if (completed.compareAndSet(false, true)) {
                    future.complete(false)
                }
                return
            }

            rateLimiter.tickBlocking()

            val request = getPaymentRequest(transactionId, paymentId, amount)

            client.newCall(request).enqueue(object : Callback {
                override fun onResponse(call: Call, response: Response) {
                    try {
                        response.use { resp ->
                            val body = try {
                                mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${resp.code}, reason: ${resp.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                            when {
                                body.result -> {
                                    if (completed.compareAndSet(false, true)) {
                                        future.complete(true)
                                        scheduledFuture?.cancel(false)
                                        
                                    }
                                }

                                else -> {
                                    if (completed.compareAndSet(false, true)) {
                                        future.complete(false)
                                        scheduledFuture?.cancel(false)
                                        
                                    }
                                }
                            }
                        }
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing response for payment $paymentId", e)
                        if (attemptsLeft.decrementAndGet() == 0) {
                            if (completed.compareAndSet(false, true)) {
                                future.complete(false)
                                
                            }
                        }
                    } finally {
                        semaphore.release()
                        
                    }
                }

                override fun onFailure(call: Call, e: IOException) {
                    try {
                        when (e) {
                            is SocketTimeoutException -> {
                                logger.error(
                                    "[$accountName] Payment socket timeout for txId: $transactionId, payment: $paymentId",
                                    e
                                )
                            }

                            is java.io.InterruptedIOException -> {
                                logger.error(
                                    "[$accountName] Payment interrupted (timeout/cancel) for txId: $transactionId, payment: $paymentId",
                                    e
                                )
                            }

                            else -> {
                                logger.error(
                                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                    e
                                )
                            }
                        }
                        if (attemptsLeft.decrementAndGet() == 0) {
                            if (completed.compareAndSet(false, true)) {
                                future.complete(false)
                            }
                        }
                    } catch (ex: Exception) {
                        logger.error("[$accountName] Error in onFailure for payment $paymentId", ex)
                        if (attemptsLeft.decrementAndGet() == 0) {
                            if (completed.compareAndSet(false, true)) {
                                future.complete(false)
                                
                            }
                        }
                    } finally {
                        semaphore.release()
                        
                    }
                }
            })
        }

        sendAttempt()

        val hedgeThresholdMs = 200L
        val minTimeForHedge = hedgeThresholdMs + 500L

        if (timeRemaining > minTimeForHedge) {
            attemptsLeft.incrementAndGet()
            scheduledFuture = scheduler.schedule({
                if (!completed.get()) {
                    logger.info("[$accountName] Sending hedged request for payment $paymentId, txId: $transactionId")
                    retryCounter.increment()
                    sendAttempt()
                }
            }, hedgeThresholdMs, TimeUnit.MILLISECONDS)
        }

        return future
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