package ru.quipy.common.utils.retry

import kotlin.reflect.KClass

inline fun <T> doRetry(
    maxAttempts: Int = 3,
    delay: Long = 1000,
    retryOn: List<KClass<out Throwable>> = listOf(Exception::class),
    recover: () -> T,
    body: () -> T,
): T {
    var currentAttempts = maxAttempts
    while (currentAttempts > 0) {
        try {
            return body()
        } catch (ex: Throwable) {
            currentAttempts = doDelayOrThrowSync(
                retryOn,
                currentAttempts,
                delay,
                ex,
            )
        }
    }
    return recover()
}

fun doDelayOrThrowSync(
    retryOn: List<KClass<out Throwable>>,
    currentAttempts: Int,
    delay: Long,
    ex: Throwable,
): Int {
    var newCurrentAttempts = currentAttempts
    val shouldRetry = retryOn.isEmpty() || retryOn.any { ex::class == it }
    if (shouldRetry) {
        newCurrentAttempts--
        if (newCurrentAttempts > 0) {
            try {
                Thread.sleep(delay)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw ex
            }
        }
    } else {
        throw ex
    }
    return newCurrentAttempts
}
