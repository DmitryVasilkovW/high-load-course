package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.impl.tokenbucket.TokenBucketRateLimiter

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        32,
        60L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(100),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler(),
    )
    private var averageProcessingTime: Long = 0
    private var rateLimitPerSec: Int = 0
    private var parallelRequests: Int = 0

    val rateLimiter = TokenBucketRateLimiter(8, 8, 1, TimeUnit.SECONDS)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (!rateLimiter.tick()) {
            val retryAfter = System.currentTimeMillis() + 30000
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Rate limit exceeded",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null,
            ).also {
                it.responseHeaders?.set("Retry-After", retryAfter.toString())
            }
        }

        if (paymentExecutor.queue.size >= paymentExecutor.queue.remainingCapacity()) {
            val retryAfter = System.currentTimeMillis() + 10000
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor queue is full",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null,
            ).also {
                it.responseHeaders?.set("Retry-After", retryAfter.toString())
            }
        }
        val createdAt = System.currentTimeMillis()

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount,
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}

class TooManyRequestsError(val millisToRetry: Long) : RuntimeException()
