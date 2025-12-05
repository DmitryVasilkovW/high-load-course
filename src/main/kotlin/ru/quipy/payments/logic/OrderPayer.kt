package ru.quipy.payments.logic


import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.impl.tokenbucket.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = Scheduler

    val rateLimiter = TokenBucketRateLimiter(500, 500, 1, TimeUnit.SECONDS)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (paymentExecutor.queue.remainingCapacity() == 0) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor queue is full",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        if (!rateLimiter.tick()) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor can't acquire a token",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        val createdAt = System.currentTimeMillis()

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
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

        process(paymentRequest, timeLeft, start, paymentId, attempt, deadline, amount, createdAt)
    }

    private fun process(
        paymentRequest: CompletableFuture<Boolean>,
        timeLeft: Long,
        start: Long,
        paymentId: UUID,
        attempt: Int,
        deadline: Long,
        amount: Int,
        createdAt: Long
    ) {
        paymentRequest
            .orTimeout(timeLeft, TimeUnit.MILLISECONDS)
            .whenCompleteAsync(
                { success, error ->
                    System.currentTimeMillis() - start

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
                },
                paymentExecutor,
            )
    }

    private fun scheduleProcess(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now

        if (timeLeft <= 0) {
            return
        }

        paymentExecutor.schedule(
            {
                retryAsync(paymentId, amount, createdAt, deadline, attempt + 1)
            },
            100L,
            TimeUnit.MILLISECONDS
        )
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }
}

object Scheduler : ScheduledThreadPoolExecutor(
    2000,
    NamedThreadFactory("payment-submission-executor")
) {
    init {
        setMaximumPoolSize(2000)
        setKeepAliveTime(0L, TimeUnit.MILLISECONDS)
        setRejectedExecutionHandler(CallerBlockingRejectedExecutionHandler())
        removeOnCancelPolicy = true
    }
}
