package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.common.utils.ratelimiter.impl.leakingbucket.LeakingBucketRateLimiter
import java.time.Duration
import java.util.*

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentAccounts.forEach {
            it.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    override fun getLeakingBucket(waitingTime: Duration): LeakingBucketRateLimiter {
        val bucketSize = paymentAccounts.sumOf {
            it.getRateLimit() * (waitingTime.toMillis() - (it.getProcessingTime().toMillis() * 2)) / 1000
        }
        return LeakingBucketRateLimiter(
            rate = paymentAccounts.sumOf { it.getRateLimit() },
            window = Duration.ofSeconds(1),
            bucketSize = bucketSize.toInt()
        )
    }

    override fun getAllAccountProperties(): List<PaymentAccountProperties> = paymentAccounts.map { it.getProperties() }
}
