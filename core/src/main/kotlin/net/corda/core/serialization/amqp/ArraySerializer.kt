package net.corda.core.serialization.amqp

import org.apache.qpid.proton.codec.Data
import java.io.NotSerializableException
import java.lang.reflect.Type

/**
 * Serialization / deserialization of arrays.
 */
open class ArraySerializer(override val type: Type, factory: SerializerFactory) : AMQPSerializer<Any> {
    override val typeDescriptor = "$DESCRIPTOR_DOMAIN:${fingerprintForType(type, factory)}"

    internal val elementType: Type = type.componentType()

    internal val typeNotation: TypeNotation = RestrictedType(type.typeName, null, emptyList(), "list", Descriptor(typeDescriptor, null), emptyList())

    override fun writeClassInfo(output: SerializationOutput) {
        if (output.writeTypeNotations(typeNotation)) {
            output.requireSerializer(elementType)
        }
    }

    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        // Write described
        data.withDescribed(typeNotation.descriptor) {
            withList {
                for (entry in obj as Array<*>) {
                    output.writeObjectOrNull(entry, this, elementType)
                }
            }
        }
    }

    override fun readObject(obj: Any, schema: Schema, input: DeserializationInput): Any {
        if (obj is List<*>) {
            return obj.map { input.readObjectOrNull(it, schema, elementType) }.toArrayOfType(elementType)
        } else throw NotSerializableException("Expected a List but found $obj")
    }

    internal fun <T> List<T>.toArrayOfType(type: Type): Any {
        val elementType = type.asClass() ?: throw NotSerializableException("Unexpected array element type $type")
        val list = this
        return java.lang.reflect.Array.newInstance(elementType, this.size).apply {
            val array = this
            for (i in 0..lastIndex) {
                java.lang.reflect.Array.set(array, i, list[i])
            }
        }
    }
}

class PrimArraySerializer(type: Type, factory: SerializerFactory) : ArraySerializer (type, factory) {
    override fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput) {
        println ("PrimArraySerializer::writeObject")
        println (obj::class.java)
        println (obj.javaClass)
        println ("element type = $elementType")
        println (typeDescriptor)

   //     println (obj as IntArray)

        data.withDescribed(typeNotation.descriptor) {
            withList {
                for (entry in obj as IntArray) {
                    println ("  write: $entry")
                    output.writeObjectOrNull(entry, this, elementType)
                }
            }
        }
    }

    override fun readObject(obj: Any, schema: Schema, input: DeserializationInput): Any {
        println ("PrimArraySerializer::readObject")
        if (obj is List<*>) {
            return obj.map { input.readObjectOrNull(it, schema, elementType) }.toArrayOfType(elementType)
        } else throw NotSerializableException("Expected a List but found $obj")
    }
}
