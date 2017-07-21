package net.corda.node.shell

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.io.Closeables
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.SettableFuture
import net.corda.core.*
import net.corda.core.flows.FlowInitiator
import net.corda.core.flows.FlowLogic
import net.corda.core.internal.*
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.StateMachineUpdate
import net.corda.core.internal.Emoji
import net.corda.core.utilities.loggerFor
import net.corda.jackson.JacksonSupport
import net.corda.jackson.StringToMethodCallParser
import net.corda.node.internal.Node
import net.corda.node.services.messaging.CURRENT_RPC_CONTEXT
import net.corda.node.services.messaging.RpcContext
import net.corda.node.services.statemachine.FlowStateMachineImpl
import net.corda.node.utilities.ANSIProgressRenderer
import net.corda.nodeapi.ArtemisMessagingComponent
import net.corda.nodeapi.User
import org.crsh.command.InvocationContext
import org.crsh.console.jline.JLineProcessor
import org.crsh.console.jline.TerminalFactory
import org.crsh.console.jline.console.ConsoleReader
import org.crsh.lang.impl.java.JavaLanguage
import org.crsh.plugin.CRaSHPlugin
import org.crsh.plugin.PluginContext
import org.crsh.plugin.PluginLifeCycle
import org.crsh.plugin.ServiceLoaderDiscovery
import org.crsh.shell.Shell
import org.crsh.shell.ShellFactory
import org.crsh.shell.impl.command.ExternalResolver
import org.crsh.text.Color
import org.crsh.text.RenderPrintWriter
import org.crsh.util.InterruptHandler
import org.crsh.util.Utils
import org.crsh.vfs.FS
import org.crsh.vfs.spi.file.FileMountFactory
import org.crsh.vfs.spi.url.ClassPathMountFactory
import rx.Observable
import rx.Subscriber
import java.io.*
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import kotlin.concurrent.thread

// TODO: Add command history.
// TODO: Command completion.
// TODO: Do something sensible with commands that return a future.
// TODO: Configure default renderers, send objects down the pipeline, add commands to do json/xml/yaml outputs.
// TODO: Add a command to view last N lines/tail/control log4j2 loggers.
// TODO: Review or fix the JVM commands which have bitrotted and some are useless.
// TODO: Get rid of the 'java' command, it's kind of worthless.
// TODO: Fix up the 'dashboard' command which has some rendering issues.
// TODO: Resurrect or reimplement the mail plugin.
// TODO: Make it notice new shell commands added after the node started.

object InteractiveShell {
    private val log = loggerFor<InteractiveShell>()
    private lateinit var node: Node

    /**
     * Starts an interactive shell connected to the local terminal. This shell gives administrator access to the node
     * internals.
     */
    fun startShell(dir: Path, runLocalShell: Boolean, runSSHServer: Boolean, node: Node) {
        this.node = node
        var runSSH = runSSHServer

        val config = Properties()
        if (runSSH) {
            // TODO: Finish and enable SSH access.
            // This means bringing the CRaSH SSH plugin into the Corda tree and applying Marek's patches
            // found in https://github.com/marekdapps/crash/commit/8a37ce1c7ef4d32ca18f6396a1a9d9841f7ff643
            // to that local copy, as CRaSH is no longer well maintained by the upstream and the SSH plugin
            // that it comes with is based on a very old version of Apache SSHD which can't handle connections
            // from newer SSH clients. It also means hooking things up to the authentication system.
            Node.printBasicNodeInfo("SSH server access is not fully implemented, sorry.")
            runSSH = false
        }

        if (runSSH) {
            // Enable SSH access. Note: these have to be strings, even though raw object assignments also work.
            config["crash.ssh.keypath"] = (dir / "sshkey").toString()
            config["crash.ssh.keygen"] = "true"
            // config["crash.ssh.port"] = node.configuration.sshdAddress.port.toString()
            config["crash.auth"] = "simple"
            config["crash.auth.simple.username"] = "admin"
            config["crash.auth.simple.password"] = "admin"
        }

        ExternalResolver.INSTANCE.addCommand("run", "Runs a method from the CordaRPCOps interface on the node.", RunShellCommand::class.java)
        ExternalResolver.INSTANCE.addCommand("flow", "Commands to work with flows. Flows are how you can change the ledger.", FlowShellCommand::class.java)
        ExternalResolver.INSTANCE.addCommand("start", "An alias for 'flow start'", StartShellCommand::class.java)
        val shell = ShellLifecycle(dir).start(config)

        if (runSSH) {
            // printBasicNodeInfo("SSH server listening on address", node.configuration.sshdAddress.toString())
        }

        // Possibly bring up a local shell in the launching terminal window, unless it's disabled.
        if (!runLocalShell)
            return
        // TODO: Automatically set up the JDBC sub-command with a connection to the database.
        val terminal = TerminalFactory.create()
        val consoleReader = ConsoleReader("Corda", FileInputStream(FileDescriptor.`in`), System.out, terminal)
        val jlineProcessor = JLineProcessor(terminal.isAnsiSupported, shell, consoleReader, System.out)
        InterruptHandler { jlineProcessor.interrupt() }.install()
        thread(name = "Command line shell processor", isDaemon = true) {
            // Give whoever has local shell access administrator access to the node.
            CURRENT_RPC_CONTEXT.set(RpcContext(User(ArtemisMessagingComponent.NODE_USER, "", setOf())))
            Emoji.renderIfSupported {
                jlineProcessor.run()
            }
        }
        thread(name = "Command line shell terminator", isDaemon = true) {
            // Wait for the shell to finish.
            jlineProcessor.closed()
            log.info("Command shell has exited")
            terminal.restore()
            node.stop()
        }
    }

    class ShellLifecycle(val dir: Path) : PluginLifeCycle() {
        fun start(config: Properties): Shell {
            val classLoader = this.javaClass.classLoader
            val classpathDriver = ClassPathMountFactory(classLoader)
            val fileDriver = FileMountFactory(Utils.getCurrentDirectory())

            val extraCommandsPath = (dir / "shell-commands").toAbsolutePath().createDirectories()
            val commandsFS = FS.Builder()
                    .register("file", fileDriver)
                    .mount("file:" + extraCommandsPath)
                    .register("classpath", classpathDriver)
                    .mount("classpath:/net/corda/node/shell/")
                    .mount("classpath:/crash/commands/")
                    .build()
            val confFS = FS.Builder()
                    .register("classpath", classpathDriver)
                    .mount("classpath:/crash")
                    .build()

            val discovery = object : ServiceLoaderDiscovery(classLoader) {
                override fun getPlugins(): Iterable<CRaSHPlugin<*>> {
                    // Don't use the Java language plugin (we may not have tools.jar available at runtime), this
                    // will cause any commands using JIT Java compilation to be suppressed. In CRaSH upstream that
                    // is only the 'jmx' command.
                    return super.getPlugins().filterNot { it is JavaLanguage }
                }
            }
            val attributes = mapOf(
                    "node" to node,
                    "services" to node.services,
                    "ops" to node.rpcOps,
                    "mapper" to yamlInputMapper
            )
            val context = PluginContext(discovery, attributes, commandsFS, confFS, classLoader)
            context.refresh()
            this.config = config
            start(context)
            return context.getPlugin(ShellFactory::class.java).create(null)
        }
    }

    private val yamlInputMapper: ObjectMapper by lazy {
        // Return a standard Corda Jackson object mapper, configured to use YAML by default and with extra
        // serializers.
        JacksonSupport.createInMemoryMapper(node.services.identityService, YAMLFactory(), true).apply {
            val rpcModule = SimpleModule()
            rpcModule.addDeserializer(InputStream::class.java, InputStreamDeserializer)
            registerModule(rpcModule)
        }
    }

    private fun createOutputMapper(factory: JsonFactory): ObjectMapper {
        return JacksonSupport.createNonRpcMapper(factory).apply({
            // Register serializers for stateful objects from libraries that are special to the RPC system and don't
            // make sense to print out to the screen. For classes we own, annotations can be used instead.
            val rpcModule = SimpleModule()
            rpcModule.addSerializer(Observable::class.java, ObservableSerializer)
            rpcModule.addSerializer(InputStream::class.java, InputStreamSerializer)
            registerModule(rpcModule)

            disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            enable(SerializationFeature.INDENT_OUTPUT)
        })
    }

    // TODO: This should become the default renderer rather than something used specifically by commands.
    private val yamlMapper by lazy { createOutputMapper(YAMLFactory()) }

    /**
     * Called from the 'flow' shell command. Takes a name fragment and finds a matching flow, or prints out
     * the list of options if the request is ambiguous. Then parses [inputData] as constructor arguments using
     * the [runFlowFromString] method and starts the requested flow using the [ANSIProgressRenderer] to draw
     * the progress tracker. Ctrl-C can be used to cancel.
     */
    @JvmStatic
    fun runFlowByNameFragment(nameFragment: String, inputData: String, output: RenderPrintWriter) {
        val matches = node.services.rpcFlows.filter { nameFragment in it.name }
        if (matches.isEmpty()) {
            output.println("No matching flow found, run 'flow list' to see your options.", Color.red)
            return
        } else if (matches.size > 1) {
            output.println("Ambigous name provided, please be more specific. Your options are:")
            matches.forEachIndexed { i, s -> output.println("${i + 1}. $s", Color.yellow) }
            return
        }

        @Suppress("UNCHECKED_CAST")
        val clazz = matches.single() as Class<FlowLogic<*>>
        try {
            // TODO Flow invocation should use startFlowDynamic.
            val fsm = runFlowFromString({ node.services.startFlow(it, FlowInitiator.Shell) }, inputData, clazz)
            // Show the progress tracker on the console until the flow completes or is interrupted with a
            // Ctrl-C keypress.
            val latch = CountDownLatch(1)
            ANSIProgressRenderer.onDone = { latch.countDown() }
            ANSIProgressRenderer.progressTracker = (fsm as FlowStateMachineImpl).logic.progressTracker
            try {
                // Wait for the flow to end and the progress tracker to notice. By the time the latch is released
                // the tracker is done with the screen.
                latch.await()
            } catch(e: InterruptedException) {
                ANSIProgressRenderer.progressTracker = null
                // TODO: When the flow framework allows us to kill flows mid-flight, do so here.
            }
        } catch(e: NoApplicableConstructor) {
            output.println("No matching constructor found:", Color.red)
            e.errors.forEach { output.println("- $it", Color.red) }
        } finally {
            InputStreamDeserializer.closeAll()
        }
    }

    class NoApplicableConstructor(val errors: List<String>) : Exception() {
        override fun toString() = (listOf("No applicable constructor for flow. Problems were:") + errors).joinToString(System.lineSeparator())
    }

    // TODO: This utility is generally useful and might be better moved to the node class, or an RPC, if we can commit to making it stable API.
    /**
     * Given a [FlowLogic] class and a string in one-line Yaml form, finds an applicable constructor and starts
     * the flow, returning the created flow logic. Useful for lightweight invocation where text is preferable
     * to statically typed, compiled code.
     *
     * See the [StringToMethodCallParser] class to learn more about limitations and acceptable syntax.
     *
     * @throws NoApplicableConstructor if no constructor could be found for the given set of types.
     */
    @Throws(NoApplicableConstructor::class)
    fun runFlowFromString(invoke: (FlowLogic<*>) -> FlowStateMachine<*>,
                          inputData: String,
                          clazz: Class<out FlowLogic<*>>,
                          om: ObjectMapper = yamlInputMapper): FlowStateMachine<*> {
        // For each constructor, attempt to parse the input data as a method call. Use the first that succeeds,
        // and keep track of the reasons we failed so we can print them out if no constructors are usable.
        val parser = StringToMethodCallParser(clazz, om)
        val errors = ArrayList<String>()
        for (ctor in clazz.constructors) {
            var paramNamesFromConstructor: List<String>? = null
            fun getPrototype(ctor: Constructor<*>): List<String> {
                val argTypes = ctor.parameterTypes.map { it.simpleName }
                val prototype = paramNamesFromConstructor!!.zip(argTypes).map { (name, type) -> "$name: $type" }
                return prototype
            }
            try {
                // Attempt construction with the given arguments.
                paramNamesFromConstructor = parser.paramNamesFromConstructor(ctor)
                val args = parser.parseArguments(clazz.name, paramNamesFromConstructor.zip(ctor.parameterTypes), inputData)
                if (args.size != ctor.parameterTypes.size) {
                    errors.add("${getPrototype(ctor)}: Wrong number of arguments (${args.size} provided, ${ctor.parameterTypes.size} needed)")
                    continue
                }
                val flow = ctor.newInstance(*args) as FlowLogic<*>
                return invoke(flow)
            } catch(e: StringToMethodCallParser.UnparseableCallException.MissingParameter) {
                errors.add("${getPrototype(ctor)}: missing parameter ${e.paramName}")
            } catch(e: StringToMethodCallParser.UnparseableCallException.TooManyParameters) {
                errors.add("${getPrototype(ctor)}: too many parameters")
            } catch(e: StringToMethodCallParser.UnparseableCallException.ReflectionDataMissing) {
                val argTypes = ctor.parameterTypes.map { it.simpleName }
                errors.add("$argTypes: <constructor missing parameter reflection data>")
            } catch(e: StringToMethodCallParser.UnparseableCallException) {
                val argTypes = ctor.parameterTypes.map { it.simpleName }
                errors.add("$argTypes: ${e.message}")
            }
        }
        throw NoApplicableConstructor(errors)
    }

    // TODO Filtering on error/success when we will have some sort of flow auditing, for now it doesn't make much sense.
    @JvmStatic
    fun runStateMachinesView(out: RenderPrintWriter): Any? {
        val proxy = node.rpcOps
        val (stateMachines, stateMachineUpdates) = proxy.stateMachinesAndUpdates()
        val currentStateMachines = stateMachines.map { StateMachineUpdate.Added(it) }
        val subscriber = FlowWatchPrintingSubscriber(out)
        stateMachineUpdates.startWith(currentStateMachines).subscribe(subscriber)
        var result: Any? = subscriber.future
        if (result is Future<*>) {
            if (!result.isDone) {
                out.cls()
                out.println("Waiting for completion or Ctrl-C ... ")
                out.flush()
            }
            try {
                result = result.get()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
            } catch (e: ExecutionException) {
                throw e.rootCause
            } catch (e: InvocationTargetException) {
                throw e.rootCause
            }
        }
        return result
    }

    @JvmStatic
    fun runRPCFromString(input: List<String>, out: RenderPrintWriter, context: InvocationContext<out Any>): Any? {
        val parser = StringToMethodCallParser(CordaRPCOps::class.java, context.attributes["mapper"] as ObjectMapper)

        val cmd = input.joinToString(" ").trim { it <= ' ' }
        if (cmd.toLowerCase().startsWith("startflow")) {
            // The flow command provides better support and startFlow requires special handling anyway due to
            // the generic startFlow RPC interface which offers no type information with which to parse the
            // string form of the command.
            out.println("Please use the 'flow' command to interact with flows rather than the 'run' command.", Color.yellow)
            return null
        }

        var result: Any? = null
        try {
            InputStreamSerializer.invokeContext = context
            val call = parser.parse(context.attributes["ops"] as CordaRPCOps, cmd)
            result = call.call()
            if (result != null && result !is kotlin.Unit && result !is Void) {
                result = printAndFollowRPCResponse(result, out)
            }
            if (result is Future<*>) {
                if (!result.isDone) {
                    out.println("Waiting for completion or Ctrl-C ... ")
                    out.flush()
                }
                try {
                    result = result.get()
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                } catch (e: ExecutionException) {
                    throw e.rootCause
                } catch (e: InvocationTargetException) {
                    throw e.rootCause
                }
            }
        } catch (e: StringToMethodCallParser.UnparseableCallException) {
            out.println(e.message, Color.red)
            out.println("Please try 'man run' to learn what syntax is acceptable")
        } catch (e: Exception) {
            out.println("RPC failed: ${e.rootCause}", Color.red)
        } finally {
            InputStreamSerializer.invokeContext = null
            InputStreamDeserializer.closeAll()
        }
        return result
    }

    private fun printAndFollowRPCResponse(response: Any?, toStream: PrintWriter): ListenableFuture<Unit>? {
        val printerFun = { obj: Any? -> yamlMapper.writeValueAsString(obj) }
        toStream.println(printerFun(response))
        toStream.flush()
        return maybeFollow(response, printerFun, toStream)
    }

    private class PrintingSubscriber(private val printerFun: (Any?) -> String, private val toStream: PrintWriter) : Subscriber<Any>() {
        private var count = 0
        val future: SettableFuture<Unit> = SettableFuture.create()

        init {
            // The future is public and can be completed by something else to indicate we don't wish to follow
            // anymore (e.g. the user pressing Ctrl-C).
            future.then { unsubscribe() }
        }

        @Synchronized
        override fun onCompleted() {
            toStream.println("Observable has completed")
            future.set(Unit)
        }

        @Synchronized
        override fun onNext(t: Any?) {
            count++
            toStream.println("Observation $count: " + printerFun(t))
            toStream.flush()
        }

        @Synchronized
        override fun onError(e: Throwable) {
            toStream.println("Observable completed with an error")
            e.printStackTrace()
            future.setException(e)
        }
    }

    // Kotlin bug: USELESS_CAST warning is generated below but the IDE won't let us remove it.
    @Suppress("USELESS_CAST", "UNCHECKED_CAST")
    private fun maybeFollow(response: Any?, printerFun: (Any?) -> String, toStream: PrintWriter): SettableFuture<Unit>? {
        // Match on a couple of common patterns for "important" observables. It's tough to do this in a generic
        // way because observables can be embedded anywhere in the object graph, and can emit other arbitrary
        // object graphs that contain yet more observables. So we just look for top level responses that follow
        // the standard "track" pattern, and print them until the user presses Ctrl-C
        if (response == null) return null

        val observable: Observable<*> = when (response) {
            is Observable<*> -> response
            is Pair<*, *> -> when {
                response.first is Observable<*> -> response.first as Observable<*>
                response.second is Observable<*> -> response.second as Observable<*>
                else -> null
            }
            else -> null
        } ?: return null

        val subscriber = PrintingSubscriber(printerFun, toStream)
        (observable as Observable<Any>).subscribe(subscriber)
        return subscriber.future
    }

    //region Extra serializers
    //
    // These serializers are used to enable the user to specify objects that aren't natural data containers in the shell,
    // and for the shell to print things out that otherwise wouldn't be usefully printable.

    private object ObservableSerializer : JsonSerializer<Observable<*>>() {
        override fun serialize(value: Observable<*>, gen: JsonGenerator, serializers: SerializerProvider) {
            gen.writeString("(observable)")
        }
    }

    // A file name is deserialized to an InputStream if found.
    object InputStreamDeserializer : JsonDeserializer<InputStream>() {
        // Keep track of them so we can close them later.
        private val streams = Collections.synchronizedSet(HashSet<InputStream>())

        override fun deserialize(p: JsonParser, ctxt: DeserializationContext): InputStream {
            val stream = object : BufferedInputStream(Files.newInputStream(Paths.get(p.text))) {
                override fun close() {
                    super.close()
                    streams.remove(this)
                }
            }
            streams += stream
            return stream
        }

        fun closeAll() {
            // Clone the set with toList() here so each closed stream can be removed from the set inside close().
            streams.toList().forEach { Closeables.closeQuietly(it) }
        }
    }

    // An InputStream found in a response triggers a request to the user to provide somewhere to save it.
    private object InputStreamSerializer : JsonSerializer<InputStream>() {
        var invokeContext: InvocationContext<*>? = null

        override fun serialize(value: InputStream, gen: JsonGenerator, serializers: SerializerProvider) {
            try {
                val toPath = invokeContext!!.readLine("Path to save stream to (enter to ignore): ", true)
                if (toPath == null || toPath.isBlank()) {
                    gen.writeString("<not saved>")
                } else {
                    val path = Paths.get(toPath)
                    path.write { value.copyTo(it) }
                    gen.writeString("<saved to: $path>")
                }
            } finally {
                try {
                    value.close()
                } catch(e: IOException) {
                    // Ignore.
                }
            }
        }
    }

    //endregion
}
