package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
) : PaymentService {
    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentAccounts.forEach {
            it.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }
}
