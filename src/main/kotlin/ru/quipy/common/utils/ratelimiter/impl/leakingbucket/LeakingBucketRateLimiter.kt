package ru.quipy.common.utils.ratelimiter.impl.leakingbucket

import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.RateLimiter

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Int>(bucketSize)

    override fun tick(): Boolean {
        return queue.offer(1)
    }

    init {
        rateLimiterScope.launch {
            while (true) {
                delay(window.toMillis())
                repeatLong(rate + 1) {
                    queue.poll()
                }
            }
        }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
    }

    private inline fun repeatLong(times: Long, action: () -> Unit) {
        var count = 0L
        while (count < times) {
            action()
            count++
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}
