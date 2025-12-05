package ru.quipy.payments.logic

import jakarta.annotation.PreDestroy
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {


    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        THREAD_POOL_SIZE,
        THREAD_POOL_SIZE,
        0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(10000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    ).apply {
        allowCoreThreadTimeOut(false)
    }

    private val activeTasksSemaphore = Semaphore(THREAD_POOL_SIZE * 2)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        try {
            if (!activeTasksSemaphore.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                logger.error("Payment system overloaded, rejecting payment $paymentId")
                throw RejectedExecutionException("Payment system overloaded")
            }

            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } catch (e: Exception) {
                    logger.error("Failed to process payment $paymentId", e)
                } finally {
                    activeTasksSemaphore.release()
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to submit payment $paymentId", e)
            throw e
        }

        return createdAt
    }

    @PreDestroy
    fun shutdown() {
        logger.info("Shutting down payment executor")
        paymentExecutor.shutdown()
        try {
            if (!paymentExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                paymentExecutor.shutdownNow()
            }
        } catch (e: InterruptedException) {
            paymentExecutor.shutdownNow()
        }
    }

    fun getActiveTasksCount(): Int = THREAD_POOL_SIZE * 2 - activeTasksSemaphore.availablePermits()
    fun getQueueSize(): Int = paymentExecutor.queue.size

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val THREAD_POOL_SIZE = 200
    }
}
