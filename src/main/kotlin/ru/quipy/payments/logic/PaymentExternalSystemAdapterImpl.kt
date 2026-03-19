package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.impl.slidingwindow.SlidingWindowRateLimiter
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import kotlin.math.pow

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    paymentProviderHostPort: String,
    token: String,
) : PaymentExternalSystemAdapter, AutoCloseable {

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rejectedCountingPolicy = RejectedExecutionHandler { r, executor ->
        if (!executor.isShutdown) {
            r.run()
        }
    }

    private val okHttpExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(parallelRequests * 2),
        NamedThreadFactory("okhttp-dispatcher-executor"),
        rejectedCountingPolicy
    )

    private val hedgeExecutor = ScheduledThreadPoolExecutor(
        4,
        NamedThreadFactory("hedge-executor-${accountName}")
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
    private val semaphore = Semaphore(parallelRequests)

    private val host = parseHost(paymentProviderHostPort)
    private val port = parsePort(paymentProviderHostPort)
    private val baseUrlComponents = mapOf(
        "serviceName" to serviceName,
        "token" to token,
        "accountName" to accountName,
    )

    private val circuitBreaker = CircuitBreakerRegistry.of(
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50f)
            .slowCallRateThreshold(50f)
            .slowCallDurationThreshold(Duration.ofMillis(500))
            .minimumNumberOfCalls(10)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(50)
            .waitDurationInOpenState(Duration.ofSeconds(1))
            .permittedNumberOfCallsInHalfOpenState(3)
            .build()
    ).circuitBreaker(accountName)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        return performPaymentAsyncWithRetry(paymentId, amount, paymentStartedAt, deadline, 1)
    }

    private fun performPaymentAsyncWithRetry(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for operation $paymentId, attempt $attempt")

        val now = now()
        if (now >= deadline) {
            return CompletableFuture.completedFuture(false)
        }

        val future = CompletableFuture<Boolean>()
        val transactionId = UUID.randomUUID()

        val protectedFuture = executeWithHedging(paymentId, amount, transactionId)

        protectedFuture.whenComplete { result, throwable ->
            val currentTime = now()
            if (throwable != null) {
                logger.error("[$accountName] Payment attempt $attempt failed for $paymentId", throwable)
                if (currentTime < deadline && attempt < MAX_RETRIES) {
                    val delayMs = calculateBackoff(attempt)
                    logger.info("[$accountName] Scheduling retry $attempt for $paymentId in ${delayMs}ms")
                    hedgeExecutor.schedule({
                        val retryFuture = performPaymentAsyncWithRetry(
                            paymentId,
                            amount,
                            paymentStartedAt,
                            deadline,
                            attempt + 1
                        )
                        retryFuture.whenComplete { retryResult, retryThrowable ->
                            if (retryThrowable != null) {
                                future.completeExceptionally(retryThrowable)
                            } else {
                                future.complete(retryResult)
                            }
                        }
                    }, delayMs, TimeUnit.MILLISECONDS)
                } else {
                    future.completeExceptionally(throwable)
                }
            } else {
                future.complete(result)
            }
        }

        return future
    }

    private fun calculateBackoff(attempt: Int): Long {
        val base = 100L
        return (base * 2.0.pow((attempt - 1).toDouble())).toLong()
    }

    private fun executeWithHedging(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID
    ): CompletableFuture<Boolean> {
        val future = CompletableFuture<Boolean>()
        val request = createRequest(paymentId, amount, transactionId)

        val firstFuture = sendRequestProtected(request, paymentId, transactionId)
        firstFuture.whenComplete { result, throwable ->
            if (!future.isDone) {
                if (throwable != null) {
                    future.completeExceptionally(throwable)
                } else {
                    future.complete(result)
                }
            }
        }

        hedgeExecutor.schedule({
            if (!future.isDone) {
                logger.warn("[$accountName] Hedged request for payment $paymentId, txId: $transactionId")
                val hedgedFuture = sendRequestProtected(request, paymentId, transactionId)
                hedgedFuture.whenComplete { result, throwable ->
                    if (!future.isDone) {
                        if (throwable != null) {
                            future.completeExceptionally(throwable)
                        } else {
                            future.complete(result)
                        }
                    }
                }
            }
        }, HEDGE_DELAY_MS, TimeUnit.MILLISECONDS)

        return future
    }

    private fun createRequest(paymentId: UUID, amount: Int, transactionId: UUID): Request {
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

    private fun sendRequestProtected(
        request: Request,
        paymentId: UUID,
        transactionId: UUID
    ): CompletableFuture<Boolean> {
        return circuitBreaker.executeCompletionStage {
            sendRequestInternal(request, paymentId, transactionId)
        }.toCompletableFuture()
    }

    private fun sendRequestInternal(
        request: Request,
        paymentId: UUID,
        transactionId: UUID,
    ): CompletableFuture<Boolean> {
        val future = CompletableFuture<Boolean>()

        try {
            semaphore.acquire()
            rateLimiter.tickBlocking()
        } catch (e: InterruptedException) {
            future.completeExceptionally(e)
            return future
        }

        logger.info("[$accountName] Sending request payment=$paymentId txId=$transactionId")

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                try {
                    response.use { resp ->
                        val body = try {
                            mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] Failed to parse response payment=$paymentId txId=$transactionId",
                                e
                            )
                            throw e
                        }
                        logger.info("[$accountName] Response received payment=$paymentId txId=$transactionId success=${body.result}")
                        future.complete(body.result)
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Error processing response payment=$paymentId txId=$transactionId", e)
                    future.completeExceptionally(e)
                } finally {
                    semaphore.release()
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                logger.error("[$accountName] Request failed payment=$paymentId txId=$transactionId", e)
                future.completeExceptionally(e)
                semaphore.release()
            }
        })

        return future
    }

    private fun parseHost(hostPort: String): String {
        val parts = hostPort.split(":")
        return if (parts.size == 2) parts[0] else hostPort
    }

    private fun parsePort(hostPort: String): Int {
        val parts = hostPort.split(":")
        return if (parts.size == 2) parts[1].toInt() else PORT
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun getProperties(): PaymentAccountProperties = properties
    override fun getRateLimit(): Long = rateLimitPerSec.toLong()
    override fun getProcessingTime(): Duration = properties.averageProcessingTime
    override fun name() = properties.accountName

    override fun close() {
        client.dispatcher.executorService.shutdown()
        hedgeExecutor.shutdown()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
        private const val PORT = 80
        private const val MAX_RETRIES = 3
        private const val HEDGE_DELAY_MS = 120L
    }
}

private fun now() = System.currentTimeMillis()
