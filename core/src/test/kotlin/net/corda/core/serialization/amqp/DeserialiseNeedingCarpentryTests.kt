package net.corda.core.serialization.amqp

import org.junit.Test
import kotlin.test.*

class DeserialiseNeedingCarpentryTests {

//    fun testName() = Thread.currentThread().stackTrace[2].methodName
//    inline fun classTestName(clazz: String) = "${this.javaClass.name}\$${testName()}\$$clazz"

    @Test
    fun oneType() {
        data class A(val a: Int, val b: String)

        val a = A(10, "20")

        var factory = SerializerFactory()
        fun serialise(clazz: Any) = SerializationOutput(factory).serialize(clazz)

        val serA = serialise(a)

//        serA.e

//        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

//        assertTrue(obj.obj is A)
//        assertEquals(1, obj.envelope.schema.types.size)
//        assertEquals(classTestName("A"), obj.envelope.schema.types.first().name)
    }
}
