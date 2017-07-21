package net.corda.nodeapi.config

import com.typesafe.config.Config
import com.typesafe.config.ConfigUtil
import net.corda.core.internal.noneOrSingle
import net.corda.core.utilities.validateX500Name
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.parseNetworkHostAndPort
import org.bouncycastle.asn1.x500.X500Name
import org.slf4j.LoggerFactory
import java.net.Proxy
import java.net.URL
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDate
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure

@Target(AnnotationTarget.PROPERTY)
annotation class OldConfig(val value: String)

// TODO Move other config parsing to use parseAs and remove this
operator fun <T> Config.getValue(receiver: Any, metadata: KProperty<*>): T {
    return getValueInternal(metadata.name, metadata.returnType)
}

fun <T : Any> Config.parseAs(clazz: KClass<T>): T {
    require(clazz.isData) { "Only Kotlin data classes can be parsed" }
    val constructor = clazz.primaryConstructor!!
    val args = constructor.parameters
            .filterNot { it.isOptional && !hasPath(it.name!!) }
            .associateBy({ it }) { param ->
                // Get the matching property for this parameter
                val property = clazz.memberProperties.first { it.name == param.name }
                val path = defaultToOldPath(property)
                getValueInternal<Any>(path, param.type)
            }
    return constructor.callBy(args)
}

inline fun <reified T : Any> Config.parseAs(): T = parseAs(T::class)

fun Config.toProperties(): Properties {
    return entrySet().associateByTo(
            Properties(),
            { ConfigUtil.splitPath(it.key).joinToString(".") },
            { it.value.unwrapped().toString() })
}

@Suppress("UNCHECKED_CAST")
private fun <T> Config.getValueInternal(path: String, type: KType): T {
    return (if (type.arguments.isEmpty()) getSingleValue(path, type) else getCollectionValue(path, type)) as T
}

private fun Config.getSingleValue(path: String, type: KType): Any? {
    if (type.isMarkedNullable && !hasPath(path)) return null
    val typeClass = type.jvmErasure
    return when (typeClass) {
        String::class -> getString(path)
        Int::class -> getInt(path)
        Long::class -> getLong(path)
        Double::class -> getDouble(path)
        Boolean::class -> getBoolean(path)
        LocalDate::class -> LocalDate.parse(getString(path))
        Instant::class -> Instant.parse(getString(path))
        NetworkHostAndPort::class -> getString(path).parseNetworkHostAndPort()
        Path::class -> Paths.get(getString(path))
        URL::class -> URL(getString(path))
        Properties::class -> getConfig(path).toProperties()
        X500Name::class -> X500Name(getString(path)).apply(::validateX500Name)
        else -> if (typeClass.java.isEnum) {
            parseEnum(typeClass.java, getString(path))
        } else {
            getConfig(path).parseAs(typeClass)
        }
    }
}

private fun Config.getCollectionValue(path: String, type: KType): Collection<Any> {
    val typeClass = type.jvmErasure
    require(typeClass == List::class || typeClass == Set::class) { "$typeClass is not supported" }
    val elementClass = type.arguments[0].type?.jvmErasure ?: throw IllegalArgumentException("Cannot work with star projection: $type")
    if (!hasPath(path)) {
        return if (typeClass == List::class) emptyList() else emptySet()
    }
    val values: List<Any> = when (elementClass) {
        String::class -> getStringList(path)
        Int::class -> getIntList(path)
        Long::class -> getLongList(path)
        Double::class -> getDoubleList(path)
        Boolean::class -> getBooleanList(path)
        LocalDate::class -> getStringList(path).map(LocalDate::parse)
        Instant::class -> getStringList(path).map(Instant::parse)
        NetworkHostAndPort::class -> getStringList(path).map { it.parseNetworkHostAndPort() }
        Path::class -> getStringList(path).map { Paths.get(it) }
        URL::class -> getStringList(path).map(::URL)
        X500Name::class -> getStringList(path).map(::X500Name)
        Properties::class -> getConfigList(path).map(Config::toProperties)
        else -> if (elementClass.java.isEnum) {
            getStringList(path).map { parseEnum(elementClass.java, it) }
        } else {
            getConfigList(path).map { it.parseAs(elementClass) }
        }
    }
    return if (typeClass == Set::class) values.toSet() else values
}

private fun Config.defaultToOldPath(property: KProperty<*>): String {
    if (!hasPath(property.name)) {
        val oldConfig = property.annotations.filterIsInstance<OldConfig>().noneOrSingle()
        if (oldConfig != null && hasPath(oldConfig.value)) {
            logger.warn("Config key ${oldConfig.value} has been deprecated and will be removed in a future release. " +
                    "Use ${property.name} instead")
            return oldConfig.value
        }
    }
    return property.name
}

@Suppress("UNCHECKED_CAST")
private fun parseEnum(enumType: Class<*>, name: String): Enum<*> = enumBridge(enumType as Class<Proxy.Type>, name) // Any enum will do

private fun <T : Enum<T>> enumBridge(clazz: Class<T>, name: String): T = java.lang.Enum.valueOf(clazz, name)

private val logger = LoggerFactory.getLogger("net.corda.nodeapi.config")
