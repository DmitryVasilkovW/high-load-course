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

    private val host = extractHost(paymentProviderHostPort)
    private val port = extractPort(paymentProviderHostPort)
    private val baseUrlComponents = mapOf(
        "serviceName" to serviceName,
        "token" to token,
        "accountName" to accountName,
    )

    private val circuitBreaker = CircuitBreakerRegistry.of(
        CircuitBreakerConfig.custom()
            .minimumNumberOfCalls(10)
            .slowCallDurationThreshold(Duration.ofMillis(500))
            .slidingWindowSize(50)
            .failureRateThreshold(50f)
            .slowCallRateThreshold(50f)
            .waitDurationInOpenState(Duration.ofSeconds(1))
            .permittedNumberOfCallsInHalfOpenState(3)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .build()
    ).circuitBreaker(accountName)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        return submitPaymentWithRetry(paymentId, amount, paymentStartedAt, deadline, 1)
    }

    private fun submitPaymentWithRetry(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for operation $paymentId, attempt $attempt")

        if (currentTimeMs() >= deadline) {
            return CompletableFuture.completedFuture(false)
        }

        val transactionId = UUID.randomUUID()
        val outcome = CompletableFuture<Boolean>()

        dispatchWithHedging(paymentId, amount, transactionId).handle { result, error ->
            when {
                error == null -> outcome.complete(result)

                currentTimeMs() < deadline && attempt < MAX_RETRIES -> {
                    val backoff = computeBackoff(attempt)
                    logger.error("[$accountName] Payment attempt $attempt failed for $paymentId", error)
                    logger.info("[$accountName] Scheduling retry $attempt for $paymentId in ${backoff}ms")
                    hedgeExecutor.schedule({
                        submitPaymentWithRetry(
                            paymentId, amount, paymentStartedAt, deadline, attempt + 1
                        ).thenAccept { outcome.complete(it) }
                            .exceptionally { ex -> outcome.completeExceptionally(ex); null }
                    }, backoff, TimeUnit.MILLISECONDS)
                }

                else -> {
                    logger.error("[$accountName] Payment attempt $attempt failed for $paymentId", error)
                    outcome.completeExceptionally(error)
                }
            }
            null
        }

        return outcome
    }

    private fun computeBackoff(attempt: Int): Long {
        val multiplier = 2.0.pow(attempt.toDouble() - 1)
        return (100L * multiplier).toLong()
    }

    private fun dispatchWithHedging(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID
    ): CompletableFuture<Boolean> {
        val outcome = CompletableFuture<Boolean>()
        val req = buildHttpRequest(paymentId, amount, transactionId)

        val propagate: (Boolean?, Throwable?) -> Unit = { res, err ->
            if (!outcome.isDone) {
                err?.let { outcome.completeExceptionally(it) } ?: outcome.complete(res)
            }
        }

        invokeWithCircuitBreaker(req, paymentId, transactionId).handle { res, err ->
            propagate(res, err); null
        }

        hedgeExecutor.schedule({
            if (!outcome.isDone) {
                logger.warn("[$accountName] Hedged request for payment $paymentId, txId: $transactionId")
                invokeWithCircuitBreaker(req, paymentId, transactionId).handle { res, err ->
                    propagate(res, err); null
                }
            }
        }, HEDGE_DELAY_MS, TimeUnit.MILLISECONDS)

        return outcome
    }

    private fun buildHttpRequest(paymentId: UUID, amount: Int, transactionId: UUID): Request {
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

    private fun invokeWithCircuitBreaker(
        request: Request,
        paymentId: UUID,
        transactionId: UUID
    ): CompletableFuture<Boolean> =
        circuitBreaker.executeCompletionStage {
            enqueueHttpCall(request, paymentId, transactionId)
        }.toCompletableFuture()

    private fun enqueueHttpCall(
        request: Request,
        paymentId: UUID,
        transactionId: UUID,
    ): CompletableFuture<Boolean> {
        val outcome = CompletableFuture<Boolean>()

        val acquired = runCatching {
            semaphore.acquire()
            rateLimiter.tickBlocking()
        }

        if (acquired.isFailure) {
            outcome.completeExceptionally(acquired.exceptionOrNull()!!)
            return outcome
        }

        logger.info("[$accountName] Sending request payment=$paymentId txId=$transactionId")

        client.newCall(request).enqueue(PaymentCallback(paymentId, transactionId, outcome))

        return outcome
    }

    private fun extractHost(hostPort: String): String {
        val parts = hostPort.split(":")
        return if (parts.size == 2) parts[0] else hostPort
    }

    private fun extractPort(hostPort: String): Int {
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

    private inner class PaymentCallback(
        private val paymentId: UUID,
        private val transactionId: UUID,
        private val outcome: CompletableFuture<Boolean>
    ) : Callback {

        override fun onResponse(call: Call, response: Response) {
            try {
                response.use { resp ->
                    val parsed = runCatching {
                        mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                    }.onFailure { e ->
                        logger.error(
                            "[$accountName] Failed to parse response payment=$paymentId txId=$transactionId",
                            e
                        )
                    }.getOrThrow()

                    logger.info("[$accountName] Response received payment=$paymentId txId=$transactionId success=${parsed.result}")
                    outcome.complete(parsed.result)
                }
            } catch (e: Exception) {
                logger.error("[$accountName] Error processing response payment=$paymentId txId=$transactionId", e)
                outcome.completeExceptionally(e)
            } finally {
                semaphore.release()
            }
        }

        override fun onFailure(call: Call, e: IOException) {
            logger.error("[$accountName] Request failed payment=$paymentId txId=$transactionId", e)
            outcome.completeExceptionally(e)
            semaphore.release()
        }
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

private fun currentTimeMs() = System.currentTimeMillis()
