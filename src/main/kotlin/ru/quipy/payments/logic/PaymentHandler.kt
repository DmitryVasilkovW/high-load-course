package ru.quipy.payments.logic

import kotlinx.coroutines.sync.Semaphore
import okhttp3.*
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.impl.slidingwindow.SlidingWindowRateLimiter
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.emptyBody
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.logger
import ru.quipy.utils.CurrentTimeMillisSupplier
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class PaymentHandler(
    private val token: String,
    private val paymentProviderHostPort: String,
    private val properties: PaymentAccountProperties,
    private val currentTimeMillisSupplier: CurrentTimeMillisSupplier,
) {
    private val accountName = properties.accountName
    private val serviceName = properties.serviceName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val scheduler = Executors.newScheduledThreadPool(
        1,
        NamedThreadFactory("hedge-scheduler-${accountName}")
    )

    private val semaphore = Semaphore(permits = properties.parallelRequests)

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

    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofMillis(1000L))
        .dispatcher(Dispatcher(okHttpExecutor).apply {
            maxRequests = parallelRequests * 2
            maxRequestsPerHost = parallelRequests * 2
        })
        .connectionPool(ConnectionPool(parallelRequests, 20, TimeUnit.SECONDS))
        .build()

    fun handle(
        paymentId: UUID,
        deadline: Long,
        amount: Int
    ): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val future = CompletableFuture<Boolean>()
        val completed = AtomicBoolean(false)
        var scheduledFuture: ScheduledFuture<*>? = null

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val startTime = currentTimeMillisSupplier.get()
        val timeRemaining = deadline - startTime
        if (timeRemaining <= 0) {
            future.complete(false)

            return future
        }

        val attemptsLeft = AtomicInteger(1)

        sendAttempt(completed, future, transactionId, paymentId, amount, scheduledFuture, attemptsLeft)

        val hedgeThresholdMs = 200L
        val minTimeForHedge = hedgeThresholdMs + 500L

        if (timeRemaining > minTimeForHedge) {
            attemptsLeft.incrementAndGet()
            scheduledFuture = scheduler.schedule({
                if (!completed.get()) {
                    logger.info("[$accountName] Sending hedged request for payment $paymentId, txId: $transactionId")
                    sendAttempt(completed, future, transactionId, paymentId, amount, scheduledFuture, attemptsLeft)
                }
            }, hedgeThresholdMs, TimeUnit.MILLISECONDS)
        }

        return future
    }

    private fun sendAttempt(
        completed: AtomicBoolean,
        future: CompletableFuture<Boolean>,
        transactionId: UUID,
        paymentId: UUID,
        amount: Int,
        scheduledFuture: ScheduledFuture<*>?,
        attemptsLeft: AtomicInteger
    ) {
        if (semaphore.tryAcquire().not()) {
            Thread.currentThread().interrupt()
            if (completed.compareAndSet(false, true)) {
                future.complete(false)
            }
            return
        }

        rateLimiter.tickBlocking()

        val request = getPaymentRequest(transactionId, paymentId, amount)
        val callback = CallBackImpl(
            transactionId,
            paymentId,
            completed,
            future,
            scheduledFuture,
            attemptsLeft,
            semaphore,
            accountName,
        )

        client.newCall(request).enqueue(callback)
    }

    private fun getPaymentRequest(transactionId: UUID, paymentId: UUID, amount: Int): Request {
        val port = parsePort(paymentProviderHostPort)
        val host = parseHost(paymentProviderHostPort)
        val baseUrlComponents = mapOf(
            "serviceName" to serviceName,
            "token" to token,
            "accountName" to accountName,
        )

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

    private fun parsePort(hostPort: String): Int {
        val parts = hostPort.split(":")
        return if (parts.size == 2) {
            parts[1].toInt()
        } else {
            PORT
        }
    }

    private fun parseHost(hostPort: String): String {
        val parts = hostPort.split(":")
        return if (parts.size == 2) {
            parts[0]
        } else {
            hostPort
        }
    }

    companion object {
        private const val PORT = 80
    }
}
