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
import ru.quipy.common.utils.ratelimiter.RateLimiter
import ru.quipy.common.utils.ratelimiter.impl.composite.CompositeRateLimiter
import ru.quipy.common.utils.ratelimiter.impl.leakingbucket.LeakingBucketRateLimiter
import ru.quipy.common.utils.ratelimiter.impl.tokenbucket.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Service
class OrderPayer {

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val compositeRateLimiter = run {
        val tokenBucket = TokenBucketRateLimiter(
            rate = 11,
            bucketMaxCapacity = 44,
            window = 1L,
            timeUnit = TimeUnit.SECONDS
        )

        val leakingBucket = LeakingBucketRateLimiter(
            rate = 11L,
            window = 1.seconds.toJavaDuration(),
            bucketSize = 16
        )

        CompositeRateLimiter(tokenBucket, leakingBucket)
    }

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        /* corePoolSize = */ 16,
        /* maximumPoolSize = */ 16,
        /* keepAliveTime = */ 0L,
        /* unit = */ TimeUnit.MILLISECONDS,
        /* workQueue = */ LinkedBlockingQueue(1500),
        /* threadFactory = */ NamedThreadFactory("payment-submission-executor"),
        /* handler = */ CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        compositeRateLimiter.requestTokenOrThrow()

        val createdAt = System.currentTimeMillis()
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount,
                )
            }
            logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }

    private fun RateLimiter.requestTokenOrThrow() {
        this.tick().takeIf { it } ?: throw HttpClientErrorException.create(
            /* statusCode = */ HttpStatus.TOO_MANY_REQUESTS,
            /* statusText = */ "Queue is full",
            /* headers = */ HttpHeaders.EMPTY,
            /* body = */ ByteArray(0),
            /* charset = */ null
        )
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }
}
