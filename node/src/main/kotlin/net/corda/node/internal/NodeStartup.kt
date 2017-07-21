package net.corda.node.internal

import com.jcabi.manifests.Manifests
import com.typesafe.config.ConfigException
import joptsimple.OptionException
import net.corda.core.*
import net.corda.core.crypto.commonName
import net.corda.core.crypto.orgName
import net.corda.core.internal.*
import net.corda.node.VersionInfo
import net.corda.core.node.services.ServiceInfo
import net.corda.core.utilities.loggerFor
import net.corda.node.ArgsParser
import net.corda.node.CmdLineOptions
import net.corda.node.SerialFilter
import net.corda.node.defaultSerialFilter
import net.corda.node.serialization.NodeClock
import net.corda.node.services.config.FullNodeConfiguration
import net.corda.node.services.transactions.bftSMaRtSerialFilter
import net.corda.node.shell.InteractiveShell
import net.corda.node.utilities.TestClock
import net.corda.node.utilities.registration.HTTPNetworkRegistrationService
import net.corda.node.utilities.registration.NetworkRegistrationHelper
import net.corda.nodeapi.internal.addShutdownHook
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.AnsiConsole
import org.slf4j.bridge.SLF4JBridgeHandler
import sun.misc.VMSupport
import java.io.RandomAccessFile
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import kotlin.system.exitProcess

/** This class is responsible for starting a Node from command line arguments. */
open class NodeStartup(val args: Array<String>) {
    companion object {
        private val logger by lazy { loggerFor<Node>() }
        val LOGS_DIRECTORY_NAME = "logs"
        val LOGS_CAN_BE_FOUND_IN_STRING = "Logs can be found in"
    }

    open fun run() {
        val startTime = System.currentTimeMillis()
        assertCanNormalizeEmptyPath()
        val (argsParser, cmdlineOptions) = parseArguments()

        // We do the single node check before we initialise logging so that in case of a double-node start it
        // doesn't mess with the running node's logs.
        enforceSingleNodeIsRunning(cmdlineOptions.baseDirectory)

        initLogging(cmdlineOptions)

        val versionInfo = getVersionInfo()

        if (cmdlineOptions.isVersion) {
            println("${versionInfo.vendor} ${versionInfo.releaseVersion}")
            println("Revision ${versionInfo.revision}")
            println("Platform Version ${versionInfo.platformVersion}")
            exitProcess(0)
        }

        // Maybe render command line help.
        if (cmdlineOptions.help) {
            argsParser.printHelp(System.out)
            exitProcess(0)
        }

        drawBanner(versionInfo)
        Node.printBasicNodeInfo(LOGS_CAN_BE_FOUND_IN_STRING, System.getProperty("log-path"))
        val conf = loadConfigFile(cmdlineOptions)
        banJavaSerialisation(conf)
        preNetworkRegistration(conf)
        maybeRegisterWithNetworkAndExit(cmdlineOptions, conf)
        logStartupInfo(versionInfo, cmdlineOptions, conf)

        try {
            cmdlineOptions.baseDirectory.createDirectories()
            startNode(conf, versionInfo, startTime, cmdlineOptions)
        } catch (e: Exception) {
            if (e.message?.startsWith("Unknown named curve:") ?: false) {
                logger.error("Exception during node startup - ${e.message}. " +
                        "This is a known OpenJDK issue on some Linux distributions, please use OpenJDK from zulu.org or Oracle JDK.")
            } else
                logger.error("Exception during node startup", e)
            exitProcess(1)
        }

        exitProcess(0)
    }

    open protected fun preNetworkRegistration(conf: FullNodeConfiguration) = Unit

    open protected fun createNode(conf: FullNodeConfiguration, versionInfo: VersionInfo, services: Set<ServiceInfo>): Node {
        return Node(conf, services, versionInfo, if (conf.useTestClock) TestClock() else NodeClock())
    }

    open protected fun startNode(conf: FullNodeConfiguration, versionInfo: VersionInfo, startTime: Long, cmdlineOptions: CmdLineOptions) {
        val advertisedServices = conf.calculateServices()
        val node = createNode(conf, versionInfo, advertisedServices)
        node.start()
        printPluginsAndServices(node)

        node.networkMapRegistrationFuture.thenMatch({
            val elapsed = (System.currentTimeMillis() - startTime) / 10 / 100.0
            // TODO: Replace this with a standard function to get an unambiguous rendering of the X.500 name.
            val name = node.info.legalIdentity.name.orgName ?: node.info.legalIdentity.name.commonName
            Node.printBasicNodeInfo("Node for \"$name\" started up and registered in $elapsed sec")

            // Don't start the shell if there's no console attached.
            val runShell = !cmdlineOptions.noLocalShell && System.console() != null
            node.startupComplete.then {
                try {
                    InteractiveShell.startShell(cmdlineOptions.baseDirectory, runShell, cmdlineOptions.sshdServer, node)
                } catch(e: Throwable) {
                    logger.error("Shell failed to start", e)
                }
            }
        }, {})
        node.run()
    }

    open protected fun logStartupInfo(versionInfo: VersionInfo, cmdlineOptions: CmdLineOptions, conf: FullNodeConfiguration) {
        logger.info("Vendor: ${versionInfo.vendor}")
        logger.info("Release: ${versionInfo.releaseVersion}")
        logger.info("Platform Version: ${versionInfo.platformVersion}")
        logger.info("Revision: ${versionInfo.revision}")
        val info = ManagementFactory.getRuntimeMXBean()
        logger.info("PID: ${info.name.split("@").firstOrNull()}")  // TODO Java 9 has better support for this
        logger.info("Main class: ${FullNodeConfiguration::class.java.protectionDomain.codeSource.location.toURI().path}")
        logger.info("CommandLine Args: ${info.inputArguments.joinToString(" ")}")
        logger.info("Application Args: ${args.joinToString(" ")}")
        logger.info("bootclasspath: ${info.bootClassPath}")
        logger.info("classpath: ${info.classPath}")
        logger.info("VM ${info.vmName} ${info.vmVendor} ${info.vmVersion}")
        logger.info("Machine: ${lookupMachineNameAndMaybeWarn()}")
        logger.info("Working Directory: ${cmdlineOptions.baseDirectory}")
        val agentProperties = VMSupport.getAgentProperties()
        if (agentProperties.containsKey("sun.jdwp.listenerAddress")) {
            logger.info("Debug port: ${agentProperties.getProperty("sun.jdwp.listenerAddress")}")
        }
        logger.info("Starting as node on ${conf.p2pAddress}")
    }

    open protected fun maybeRegisterWithNetworkAndExit(cmdlineOptions: CmdLineOptions, conf: FullNodeConfiguration) {
        if (!cmdlineOptions.isRegistration) return
        println()
        println("******************************************************************")
        println("*                                                                *")
        println("*       Registering as a new participant with Corda network      *")
        println("*                                                                *")
        println("******************************************************************")
        NetworkRegistrationHelper(conf, HTTPNetworkRegistrationService(conf.certificateSigningService)).buildKeystore()
        exitProcess(0)
    }

    open protected fun loadConfigFile(cmdlineOptions: CmdLineOptions): FullNodeConfiguration {
        val conf = try {
            cmdlineOptions.loadConfig()
        } catch (e: ConfigException) {
            println("Unable to load the configuration file: ${e.rootCause.message}")
            exitProcess(2)
        }
        return conf
    }

    open protected fun banJavaSerialisation(conf: FullNodeConfiguration) {
        SerialFilter.install(if (conf.bftReplicaId != null) ::bftSMaRtSerialFilter else ::defaultSerialFilter)
    }

    open protected fun getVersionInfo(): VersionInfo {
        // Manifest properties are only available if running from the corda jar
        fun manifestValue(name: String): String? = if (Manifests.exists(name)) Manifests.read(name) else null

        val versionInfo = VersionInfo(
                manifestValue("Corda-Platform-Version")?.toInt() ?: 1,
                manifestValue("Corda-Release-Version") ?: "Unknown",
                manifestValue("Corda-Revision") ?: "Unknown",
                manifestValue("Corda-Vendor") ?: "Unknown"
        )
        return versionInfo
    }

    private fun enforceSingleNodeIsRunning(baseDirectory: Path) {
        // Write out our process ID (which may or may not resemble a UNIX process id - to us it's just a string) to a
        // file that we'll do our best to delete on exit. But if we don't, it'll be overwritten next time. If it already
        // exists, we try to take the file lock first before replacing it and if that fails it means we're being started
        // twice with the same directory: that's a user error and we should bail out.
        val pidFile = (baseDirectory / "process-id").toFile()
        pidFile.createNewFile()
        pidFile.deleteOnExit()
        val pidFileRw = RandomAccessFile(pidFile, "rw")
        val pidFileLock = pidFileRw.channel.tryLock()
        if (pidFileLock == null) {
            println("It appears there is already a node running with the specified data directory $baseDirectory")
            println("Shut that other node down and try again. It may have process ID ${pidFile.readText()}")
            System.exit(1)
        }
        // Avoid the lock being garbage collected. We don't really need to release it as the OS will do so for us
        // when our process shuts down, but we try in stop() anyway just to be nice.
        addShutdownHook {
            pidFileLock.release()
        }
        val ourProcessID: String = ManagementFactory.getRuntimeMXBean().name.split("@")[0]
        pidFileRw.setLength(0)
        pidFileRw.write(ourProcessID.toByteArray())
    }

    private fun parseArguments(): Pair<ArgsParser, CmdLineOptions> {
        val argsParser = ArgsParser()
        val cmdlineOptions = try {
            argsParser.parse(*args)
        } catch (ex: OptionException) {
            println("Invalid command line arguments: ${ex.message}")
            argsParser.printHelp(System.out)
            exitProcess(1)
        }
        return Pair(argsParser, cmdlineOptions)
    }

    open protected fun initLogging(cmdlineOptions: CmdLineOptions) {
        val loggingLevel = cmdlineOptions.loggingLevel.name.toLowerCase(Locale.ENGLISH)
        System.setProperty("defaultLogLevel", loggingLevel) // These properties are referenced from the XML config file.
        if (cmdlineOptions.logToConsole) {
            System.setProperty("consoleLogLevel", loggingLevel)
            Node.renderBasicInfoToConsole = false
        }
        System.setProperty("log-path", (cmdlineOptions.baseDirectory / LOGS_DIRECTORY_NAME).toString())
        SLF4JBridgeHandler.removeHandlersForRootLogger() // The default j.u.l config adds a ConsoleHandler.
        SLF4JBridgeHandler.install()
    }

    private fun lookupMachineNameAndMaybeWarn(): String {
        val start = System.currentTimeMillis()
        val hostName: String = InetAddress.getLocalHost().hostName
        val elapsed = System.currentTimeMillis() - start
        if (elapsed > 1000 && hostName.endsWith(".local")) {
            // User is probably on macOS and experiencing this problem: http://stackoverflow.com/questions/10064581/how-can-i-eliminate-slow-resolving-loading-of-localhost-virtualhost-a-2-3-secon
            //
            // Also see https://bugs.openjdk.java.net/browse/JDK-8143378
            val messages = listOf(
                    "Your computer took over a second to resolve localhost due an incorrect configuration. Corda will work but start very slowly until this is fixed. ",
                    "Please see https://docs.corda.net/troubleshooting.html#slow-localhost-resolution for information on how to fix this. ",
                    "It will only take a few seconds for you to resolve."
            )
            logger.warn(messages.joinToString(""))
            Emoji.renderIfSupported {
                print(Ansi.ansi().fgBrightRed())
                messages.forEach {
                    println("${Emoji.sleepingFace}$it")
                }
                print(Ansi.ansi().reset())
            }
        }
        return hostName
    }

    private fun assertCanNormalizeEmptyPath() {
        // Check we're not running a version of Java with a known bug: https://github.com/corda/corda/issues/83
        try {
            Paths.get("").normalize()
        } catch (e: ArrayIndexOutOfBoundsException) {
            Node.failStartUp("You are using a version of Java that is not supported (${System.getProperty("java.version")}). Please upgrade to the latest version.")
        }
    }

    private fun printPluginsAndServices(node: Node) {
        node.configuration.extraAdvertisedServiceIds.let {
            if (it.isNotEmpty()) Node.printBasicNodeInfo("Providing network services", it.joinToString())
        }
        val plugins = node.pluginRegistries
                .map { it.javaClass.name }
                .filterNot { it.startsWith("net.corda.node.") || it.startsWith("net.corda.core.") || it.startsWith("net.corda.nodeapi.") }
                .map { it.substringBefore('$') }
        if (plugins.isNotEmpty())
            Node.printBasicNodeInfo("Loaded plugins", plugins.joinToString())
    }

    open fun drawBanner(versionInfo: VersionInfo) {
        // This line makes sure ANSI escapes work on Windows, where they aren't supported out of the box.
        AnsiConsole.systemInstall()

        Emoji.renderIfSupported {
            val messages = arrayListOf(
                    "The only distributed ledger that pays\nhomage to Pac Man in its logo.",
                    "You know, I was a banker\nonce ... but I lost interest. ${Emoji.bagOfCash}",
                    "It's not who you know, it's who you know\nknows what you know you know.",
                    "It runs on the JVM because QuickBasic\nis apparently not 'professional' enough.",
                    "\"It's OK computer, I go to sleep after\ntwenty minutes of inactivity too!\"",
                    "It's kind of like a block chain but\ncords sounded healthier than chains.",
                    "Computer science and finance together.\nYou should see our crazy Christmas parties!",
                    "I met my bank manager yesterday and asked\nto check my balance ... he pushed me over!",
                    "A banker with nobody around may find\nthemselves .... a-loan! <applause>",
                    "Whenever I go near my bank I get\nwithdrawal symptoms ${Emoji.coolGuy}",
                    "There was an earthquake in California,\na local bank went into de-fault.",
                    "I asked for insurance if the nearby\nvolcano erupted. They said I'd be covered.",
                    "I had an account with a bank in the\nNorth Pole, but they froze all my assets ${Emoji.santaClaus}",
                    "Check your contracts carefully. The fine print\nis usually a clause for suspicion ${Emoji.santaClaus}",
                    "Some bankers are generous ...\nto a vault! ${Emoji.bagOfCash} ${Emoji.coolGuy}",
                    "What you can buy for a dollar these\ndays is absolute non-cents! ${Emoji.bagOfCash}",
                    "Old bankers never die, they\njust... pass the buck",
                    "I won $3M on the lottery so I donated a quarter\nof it to charity. Now I have $2,999,999.75.",
                    "There are two rules for financial success:\n1) Don't tell everything you know.",
                    "Top tip: never say \"oops\", instead\nalways say \"Ah, Interesting!\"",
                    "Computers are useless. They can only\ngive you answers.  -- Picasso"
            )
            if (Emoji.hasEmojiTerminal)
                messages += "Kind of like a regular database but\nwith emojis, colours and ascii art. ${Emoji.coolGuy}"
            val (msg1, msg2) = messages.randomOrNull()!!.split('\n')

            println(Ansi.ansi().newline().fgBrightRed().a(
                    """   ______               __""").newline().a(
                    """  / ____/     _________/ /___ _""").newline().a(
                    """ / /     __  / ___/ __  / __ `/         """).fgBrightBlue().a(msg1).newline().fgBrightRed().a(
                    """/ /___  /_/ / /  / /_/ / /_/ /          """).fgBrightBlue().a(msg2).newline().fgBrightRed().a(
                    """\____/     /_/   \__,_/\__,_/""").reset().newline().newline().fgBrightDefault().bold().
                    a("--- ${versionInfo.vendor} ${versionInfo.releaseVersion} (${versionInfo.revision.take(7)}) -----------------------------------------------").
                    newline().
                    newline().
                    a("${Emoji.books}New! ").reset().a("Training now available worldwide, see https://corda.net/corda-training/").
                    newline().
                    reset())
        }
    }
}