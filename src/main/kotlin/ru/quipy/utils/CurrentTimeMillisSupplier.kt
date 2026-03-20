package ru.quipy.utils

import org.springframework.stereotype.Service
import java.util.function.Supplier

@Service
class CurrentTimeMillisSupplier : Supplier<Long> {
    override fun get(): Long =
        System.currentTimeMillis()
}
