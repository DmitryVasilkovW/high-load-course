package ru.quipy.payments.logic

import kotlinx.coroutines.sync.Semaphore
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Response
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.logger
import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.Companion.mapper
import java.io.IOException
import java.io.InterruptedIOException
import java.net.SocketTimeoutException
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class CallBackImpl(
    private val transactionId: UUID?,
    private val paymentId: UUID,
    private val completed: AtomicBoolean,
    private val future: CompletableFuture<Boolean>,
    private val scheduledFuture: ScheduledFuture<*>?,
    private val attemptsLeft: AtomicInteger,
    private val semaphore: Semaphore,
    private val accountName: String,
) : Callback {

    override fun onResponse(call: Call, response: Response) {
        try {
            response.use { resp ->
                val body = try {
                    mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${resp.code}, reason: ${resp.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                when {
                    body.result -> {
                        if (completed.compareAndSet(false, true)) {
                            future.complete(true)
                            scheduledFuture?.cancel(false)

                        }
                    }

                    else -> {
                        if (completed.compareAndSet(false, true)) {
                            future.complete(false)
                            scheduledFuture?.cancel(false)

                        }
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("[$accountName] Error processing response for payment $paymentId", e)
            if (attemptsLeft.decrementAndGet() == 0) {
                if (completed.compareAndSet(false, true)) {
                    future.complete(false)

                }
            }
        } finally {
            semaphore.release()
        }
    }

    override fun onFailure(call: Call, e: IOException) {
        try {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error(
                        "[$accountName] Payment socket timeout for txId: $transactionId, payment: $paymentId",
                        e
                    )
                }

                is InterruptedIOException -> {
                    logger.error(
                        "[$accountName] Payment interrupted (timeout/cancel) for txId: $transactionId, payment: $paymentId",
                        e
                    )
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        e
                    )
                }
            }
            if (attemptsLeft.decrementAndGet() == 0) {
                if (completed.compareAndSet(false, true)) {
                    future.complete(false)
                }
            }
        } catch (ex: Exception) {
            logger.error("[$accountName] Error in onFailure for payment $paymentId", ex)
            if (attemptsLeft.decrementAndGet() == 0) {
                if (completed.compareAndSet(false, true)) {
                    future.complete(false)

                }
            }
        } finally {
            semaphore.release()
        }
    }
}
