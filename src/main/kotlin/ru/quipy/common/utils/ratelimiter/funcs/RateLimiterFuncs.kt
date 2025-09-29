package ru.quipy.common.utils.ratelimiter.funcs

import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import java.time.Duration
import java.util.concurrent.TimeUnit

fun makeRateLimiter(accountName: String, rate: Int, timeUnit: TimeUnit = TimeUnit.SECONDS): RateLimiter {
    val config = RateLimiterConfig.custom()
        .limitRefreshPeriod(if (timeUnit == TimeUnit.SECONDS) Duration.ofSeconds(1) else Duration.ofMinutes(1))
        .limitForPeriod(rate)
        .timeoutDuration(Duration.ofMillis(5))
        .build()

    val rateLimiterRegistry = RateLimiterRegistry.of(config)
    return rateLimiterRegistry.rateLimiter("rateLimiter:${accountName}")
}
