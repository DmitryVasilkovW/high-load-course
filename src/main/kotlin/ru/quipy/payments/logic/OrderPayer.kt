package ru.quipy.payments.logic

import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.future.await
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.common.utils.ratelimiter.impl.tokenbucket.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Service
class OrderPayer {

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentScope = CoroutineScope(
        Dispatchers.IO.limitedParallelism(500) +
                SupervisorJob() +
                CoroutineExceptionHandler { _, throwable ->
                    logger.error("Unhandled exception in payment scope", throwable)
                }
    )

    private val paymentQueue = Channel<PaymentTask>(capacity = Channel.UNLIMITED)

    private val rateLimiter = TokenBucketRateLimiter(1100, 5000, 1, TimeUnit.SECONDS)

    private val queueCounter = AtomicInteger(0)
    private val activeTasksCounter = AtomicInteger(0)

    init {
        startPaymentProcessors()
        startQueueMonitor()
    }

    private fun startPaymentProcessors() {
        val processorCount = Runtime.getRuntime().availableProcessors() * 4
        repeat(processorCount) { processorId ->
            paymentScope.launch {
                for (task in paymentQueue) {
                    try {
                        processPaymentTask(task)
                    } catch (e: Exception) {
                        logger.error("Processor $processorId failed to process task: ${task.paymentId}", e)
                    }
                }
            }
        }
    }

    private fun startQueueMonitor() {
        paymentScope.launch {
            while (isActive) {
                delay(1000)
                val queueSize = queueCounter.get()
                val activeTasks = activeTasksCounter.get()

                if (queueSize > 1000 || activeTasks > 100) {
                    logger.warn("Payment system under load - Queue: $queueSize, Active: $activeTasks")
                }
            }
        }
    }

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

        val currentQueueSize = queueCounter.get()
        if (currentQueueSize > 10000) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor queue is full",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        val createdAt = System.currentTimeMillis()

        paymentScope.launch {
            val task = PaymentTask(
                orderId = orderId,
                amount = amount,
                paymentId = paymentId,
                deadline = deadline,
                createdAt = createdAt
            )

            try {
                queueCounter.incrementAndGet()
                paymentQueue.send(task)
            } catch (e: Exception) {
                queueCounter.decrementAndGet()
                logger.error("Failed to send payment $paymentId to queue", e)
            }
        }

        return createdAt
    }

    private suspend fun processPaymentTask(task: PaymentTask) {
        activeTasksCounter.incrementAndGet()
        queueCounter.decrementAndGet()

        try {
            val createdEvent = paymentESService.create {
                it.create(task.paymentId, task.orderId, task.amount)
            }
            logger.trace("Payment ${createdEvent.paymentId} for order ${task.orderId} created.")

            retryAsync(task, attempt = 1)
        } catch (e: Exception) {
            logger.error("Failed to create payment ${task.paymentId}", e)
        } finally {
            activeTasksCounter.decrementAndGet()
        }
    }

    private fun retryAsync(task: PaymentTask, attempt: Int) {
        paymentScope.launch {
            try {
                processRetry(task, attempt)
            } catch (e: Exception) {
                logger.error("Retry failed for payment ${task.paymentId}", e)
            }
        }
    }

    private suspend fun processRetry(task: PaymentTask, attempt: Int) {
        val now = System.currentTimeMillis()
        val timeLeft = task.deadline - now

        if (timeLeft <= 0) {
            logger.warn("Payment ${task.paymentId} attempt #$attempt aborted: deadline exceeded")
            return
        }

        val paymentRequest = paymentService.submitPaymentRequest(task.paymentId, task.amount, task.createdAt, task.deadline)
        val start = System.currentTimeMillis()

        try {
            val success = withTimeoutOrNull(timeLeft) {
                paymentRequest.await()
            }

            val processingTime = System.currentTimeMillis() - start

            when {
                success == null -> {
                    logger.warn(
                        "Payment ${task.paymentId} attempt #$attempt timed out, " +
                                "timeLeft=${task.deadline - System.currentTimeMillis()}ms"
                    )
                    scheduleRetry(task, attempt)
                }
                success == true -> {
                    logger.info("Payment ${task.paymentId} attempt #$attempt succeeded in ${processingTime}ms")
                }
                else -> {
                    logger.info("Payment ${task.paymentId} attempt #$attempt returned failure in ${processingTime}ms")
                    scheduleRetry(task, attempt)
                }
            }
        } catch (e: Exception) {
            logger.warn(
                "Payment ${task.paymentId} attempt #$attempt failed: ${e.message}, " +
                        "timeLeft=${task.deadline - System.currentTimeMillis()}ms"
            )
            scheduleRetry(task, attempt)
        }
    }

    private fun scheduleRetry(task: PaymentTask, attempt: Int) {
        paymentScope.launch {
            val now = System.currentTimeMillis()
            val timeLeft = task.deadline - now

            if (timeLeft <= 0) {
                return@launch
            }

            val delayMs = minOf(100L * (1L shl (attempt - 1)), 5000L)

            delay(delayMs)
            retryAsync(task, attempt + 1)
        }
    }

    @PreDestroy
    fun shutdown() {
        logger.info("Shutting down OrderPayer. Queue: ${queueCounter.get()}, Active: ${activeTasksCounter.get()}")
        paymentScope.cancel()
        paymentQueue.close()
    }


    data class PaymentTask(
        val orderId: UUID,
        val amount: Int,
        val paymentId: UUID,
        val deadline: Long,
        val createdAt: Long
    )

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }
}