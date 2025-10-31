package ru.quipy.common.utils.retry

import kotlinx.coroutines.delay

suspend inline fun doRetry(
    maxAttempts: Int = 3,
    delay: Long = 1000,
    recover: () -> Unit = {},
    body: () -> Unit,
) {
    var currentAttempts = maxAttempts
    while (currentAttempts > 0) {
        try {
            body()
            return
        } catch (_: Exception) {
            currentAttempts--
            delay(delay)
        }
    }
    recover()
}
