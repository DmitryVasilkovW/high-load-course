package ru.quipy.common.utils.retry

import kotlin.reflect.KClass
import kotlinx.coroutines.delay

suspend inline fun doRetry(
    maxAttempts: Int = 3,
    delay: Long = 1000,
    retryOn: List<KClass<out Throwable>> = listOf(Exception::class),
    recover: () -> Unit = {},
    body: () -> Unit,
) {
    var currentAttempts = maxAttempts
    while (currentAttempts > 0) {
        try {
            body()
            return
        } catch (ex: Throwable) {
            currentAttempts = doDelayOrThrow(
                retryOn,
                currentAttempts,
                delay,
                ex,
            )
        }
    }
    recover()
}

suspend fun doDelayOrThrow(
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
            delay(delay)
        }
    } else {
        throw ex
    }
    return newCurrentAttempts
}
