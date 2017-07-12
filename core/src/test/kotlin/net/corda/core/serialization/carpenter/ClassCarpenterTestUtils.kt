package net.corda.core.serialization.carpenter.test

import net.corda.core.serialization.amqp.SerializerFactory
import net.corda.core.serialization.amqp.SerializationOutput

open class AmqpCarpenterBase {
    var factory = SerializerFactory()

    fun serialise(clazz: Any) = SerializationOutput(factory).serialize(clazz)
    fun testName() = Thread.currentThread().stackTrace[2].methodName
    inline fun classTestName(clazz: String) = "${this.javaClass.name}\$${testName()}\$$clazz"
}

