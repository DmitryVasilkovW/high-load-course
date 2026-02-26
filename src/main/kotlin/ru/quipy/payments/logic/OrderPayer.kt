package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.impl.tokenbucket.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.*

@Service
class OrderPayer {
    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val queueSize: Int = CORE_POOL_SIZE * QUEUE_SIZE_MULTIPLIER

    private val immediateExecutor: ThreadPoolExecutor = ThreadPoolExecutor(
        /* corePoolSize = */ CORE_POOL_SIZE,
        /* maximumPoolSize = */ MAX_POOL_SIZE,
        /* keepAliveTime = */ 0L,
        /* unit = */ TimeUnit.MILLISECONDS,
        /* workQueue = */ LinkedBlockingQueue(queueSize),
        /* threadFactory = */ NamedThreadFactory("order-immediate-executor")
    ) { _, executor ->
        throw RejectedExecutionException("Task rejected from $executor")
    }

    private val scheduledExecutor: ScheduledThreadPoolExecutor = ScheduledThreadPoolExecutor(
        10,
        NamedThreadFactory("order-scheduled-executor")
    ).also { executor ->
        executor.removeOnCancelPolicy = true
        executor.rejectedExecutionHandler = RejectedExecutionHandler { r, _ ->
            logger.error("Scheduled task rejected: $r")
        }
    }

    private val scheduledTasksSemaphore: Semaphore = Semaphore(MAX_SCHEDULED_TASKS)

    val rateLimiter: TokenBucketRateLimiter = TokenBucketRateLimiter(
        rate = 4000,
        bucketMaxCapacity = 5000,
        window = 1,
        timeUnit = TimeUnit.SECONDS
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (!rateLimiter.tick()) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor can't acquire a token",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        if (immediateExecutor.queue.size >= queueSize) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor queue is full",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        val createdAt = System.currentTimeMillis()

        try {
            immediateExecutor.submit {
                val createdEvent = paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
            }
        } catch (_: RejectedExecutionException) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor overloaded, please retry later",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        return createdAt
    }

    private fun retryAsync(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now

        if (timeLeft <= 0) {
            logger.warn("Payment $paymentId attempt #$attempt aborted: deadline exceeded")
            return
        }

        val paymentRequest = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        val start = System.currentTimeMillis()

        process(paymentRequest, timeLeft, paymentId, attempt, deadline, amount, createdAt)
    }

    private fun process(
        paymentRequest: CompletableFuture<Boolean>,
        timeLeft: Long,
        paymentId: UUID,
        attempt: Int,
        deadline: Long,
        amount: Int,
        createdAt: Long
    ) {
        paymentRequest
            .orTimeout(timeLeft, TimeUnit.MILLISECONDS)
            .whenCompleteAsync({ success, error ->
                when {
                    error != null -> {
                        logger.warn(
                            "Payment $paymentId attempt #$attempt failed: ${error.message}, " +
                                    "timeLeft=${deadline - System.currentTimeMillis()}ms"
                        )
                        scheduleProcess(paymentId, amount, createdAt, deadline, attempt)
                    }

                    success == true -> {
                        logger.info("Payment $paymentId attempt #$attempt succeeded")
                    }

                    else -> {
                        logger.info("Payment $paymentId attempt #$attempt returned failure")
                        scheduleProcess(paymentId, amount, createdAt, deadline, attempt)
                    }
                }
            }, immediateExecutor)
    }

    private fun scheduleProcess(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int,
    ) {
        if (deadline <= System.currentTimeMillis()) return

        with(scheduledTasksSemaphore) {
            if (!tryAcquire()) {
                logger.error("Too many scheduled retries for payment $paymentId, dropping retry #${attempt + 1}")
                return
            }

            try {
                scheduledExecutor.schedule(
                    {
                        try {
                            retryAsync(paymentId, amount, createdAt, deadline, attempt + 1)
                        } finally {
                            release()
                        }
                    },
                    100L,
                    TimeUnit.MILLISECONDS,
                )
            } catch (_: RejectedExecutionException) {
                release()
                logger.error("Failed to schedule retry for payment $paymentId attempt #${attempt + 1}: queue full")
            }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val QUEUE_SIZE_MULTIPLIER = 11
        private const val MAX_SCHEDULED_TASKS = 4000
        private const val CORE_POOL_SIZE = 100
        private const val MAX_POOL_SIZE = 100
    }
}
