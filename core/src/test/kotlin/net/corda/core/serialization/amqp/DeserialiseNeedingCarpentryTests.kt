package net.corda.core.serialization.amqp

import org.junit.Test
import kotlin.test.*
import net.corda.core.serialization.test.mangleName
import org.apache.qpid.proton.codec.Data

class TestSerializationOutput(val strings : List<String>) : SerializationOutput() {
    override fun putObject(schema: Schema, data: Data) {
        super.putObject(schema.mangleName(strings), data)
    }
}

class DeserialiseNeedingCarpentryTests {
    fun testName() = Thread.currentThread().stackTrace[2].methodName
    inline fun classTestName(clazz: String) = "${this.javaClass.name}\$${testName()}\$$clazz"

    @Test
    fun oneType() {
        data class A(val a: Int, val b: String)

        val a = A(10, "20")

        fun serialise(clazz: Any) = TestSerializationOutput(listOf(classTestName("A"))).serialize(clazz)
        val serA = serialise(a)
        val deserA = DeserializationInput().deserialize(serA)


//        serA.e

//        val obj = DeserializationInput(factory).deserializeAndReturnEnvelope(serialise(a))

//        assertTrue(obj.obj is A)
//        assertEquals(1, obj.envelope.schema.types.size)
//        assertEquals(classTestName("A"), obj.envelope.schema.types.first().name)
    }
}
