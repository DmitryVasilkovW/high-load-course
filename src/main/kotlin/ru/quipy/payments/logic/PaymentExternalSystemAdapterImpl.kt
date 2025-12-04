package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.max

// Advice: always treat time as a Duration
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
    private val processingTime = properties.averageProcessingTime

    private val client = OkHttpClient.Builder()
        .callTimeout(30, TimeUnit.SECONDS)
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .writeTimeout(5, TimeUnit.SECONDS)
        .dispatcher(Dispatcher().apply {
            maxRequests = 1500
            maxRequestsPerHost = 1500
        })
        .connectionPool(
            ConnectionPool(
                maxIdleConnections = 200,
                keepAliveDuration = 5,
                timeUnit = TimeUnit.MINUTES
            )
        )
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

    private val executorService: ExecutorService = Executors.newFixedThreadPool(parallelRequests)
    private val semaphore = Semaphore(parallelRequests)

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

    override fun getRateLimit(): Long = rateLimitPerSec.toLong()

    override fun getProcessingTime(): Duration = processingTime

    @Suppress("SwallowedException")
    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        httpRequestsTotalAccountCounter.increment()
        val transactionId = UUID.randomUUID()

        val future = CompletableFuture<Boolean>()

        logger.warn(
            "[{}] Submitting payment request for payment {}",
            accountName,
            paymentId,
        )

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info(
            "[{}] Submit: {} , txId: {}",
            accountName,
            paymentId,
            transactionId,
        )

        CompletableFuture.runAsync({
            try {
                handleExternalPaymentProcessingRequest(transactionId, paymentId, amount, future)
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
        }, executorService)

        return future
    }

    private fun handleExternalPaymentProcessingRequest(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
        future: CompletableFuture<Boolean>
    ) {
        try {
            val result: Boolean = doRetry(
                maxAttempts = 3,
                delay = delay,
                retryOn = listOf(SocketTimeoutException::class, InterruptedIOException::class, Exception::class),
                recover = {
                    logError(paymentId, transactionId)
                    false
                },
            ) {
                processPayment(transactionId, paymentId, amount)
            } as Boolean

            future.complete(result)
        } catch (e: Exception) {
            logError(paymentId, transactionId)
            future.completeExceptionally(e)
        }
    }

    private fun logError(paymentId: UUID, transactionId: UUID) {
        paymentESService.update(paymentId) {
            it.logProcessing(
                false,
                now(),
                transactionId,
                reason = "All retry attempts failed",
            )
        }
    }

    @Suppress("NestedBlockDepth")
    private fun processPayment(
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
    ): Boolean {
        incomingRegCounter.increment()
        val startTime = now()
        var success = false

        try {
            semaphore.acquire()
            val request = getPaymentRequest(transactionId, paymentId, amount)
            outgoingReqCounter.increment()

            rateLimiter.tick()
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error(
                        "[{}] [ERROR] Payment processed for txId: {}, payment: {}, result code: {}, reason: {}",
                        accountName,
                        transactionId,
                        paymentId,
                        response.code,
                        response.body?.string(),
                    )
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn(
                    "[{}] Payment processed for txId: {}, payment: {}, succeeded: {}, message: {}",
                    accountName,
                    transactionId,
                    paymentId,
                    body.result,
                    body.message,
                )

                success = body.result

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
            httpHandledRequestsTotalAccountCounter.increment()
            val processingTime = now() - startTime
            outgoingRequestProcessingTimeDistributionSummary.record(processingTime.toDouble())
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
        } catch (e: InterruptedIOException) {
            retryCounter.increment()
            logger.error(
                "[{}] Payment interrupted for txId: {}, payment: {}",
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

        return success
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

    override fun name() = properties.accountName

    override fun close() {
        executorService.shutdown()
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow()
            }
        } catch (e: InterruptedException) {
            executorService.shutdownNow()
            Thread.currentThread().interrupt()
        }
    }

    private fun now() = System.currentTimeMillis()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()

        private const val PORT = 80
    }
}