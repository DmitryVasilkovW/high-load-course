package ru.quipy.common.utils.ratelimiter.impl.slidingwindow

import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.RateLimiter

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val sum = AtomicLong(0)
    private val queue = PriorityBlockingQueue<Measure>(QUEUE_CAPACITY)
    private val windowMillis = window.toMillis()

    init {
        rateLimiterScope.launch {
            while (true) {
                val head = queue.peek()
                val winStart = System.currentTimeMillis() - windowMillis
                when {
                    head == null -> delay(1L)
                    head.timestamp > winStart -> delay(head.timestamp - winStart)

                    else -> {
                        sum.addAndGet(-1)
                        queue.take()
                    }
                }
            }
        }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    }

    override fun tick(): Boolean {
        while (true) {
            val curSum = sum.get()
            if (curSum >= rate) return false
            if (sum.compareAndSet(curSum, curSum + 1)) {
                queue.add(Measure(1, System.currentTimeMillis()))
                return true
            }
        }
    }

    fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(MS_TO_WAIT)
        }
    }

    data class Measure(
        val value: Long,
        val timestamp: Long,
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    companion object {
        private const val MS_TO_WAIT = 10L
        private const val QUEUE_CAPACITY = 10_000
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}
