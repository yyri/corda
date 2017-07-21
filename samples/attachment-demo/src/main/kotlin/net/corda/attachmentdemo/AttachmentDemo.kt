package net.corda.attachmentdemo

import co.paralleluniverse.fibers.Suspendable
import joptsimple.OptionParser
import net.corda.client.rpc.CordaRPCClient
import net.corda.core.contracts.Contract
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.TransactionType
import net.corda.core.crypto.SecureHash
import net.corda.core.flows.FinalityFlow
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StartableByRPC
import net.corda.core.getOrThrow
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.internal.Emoji
import net.corda.core.internal.InputStreamAndHash
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.startTrackedFlow
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.ProgressTracker
import net.corda.testing.DUMMY_BANK_B
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.driver.poll
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.Executors
import java.util.jar.JarInputStream
import javax.servlet.http.HttpServletResponse.SC_OK
import javax.ws.rs.core.HttpHeaders.CONTENT_DISPOSITION
import javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM
import kotlin.system.exitProcess

internal enum class Role {
    SENDER,
    RECIPIENT
}

fun main(args: Array<String>) {
    val parser = OptionParser()

    val roleArg = parser.accepts("role").withRequiredArg().ofType(Role::class.java).required()
    val options = try {
        parser.parse(*args)
    } catch (e: Exception) {
        println(e.message)
        printHelp(parser)
        exitProcess(1)
    }

    val role = options.valueOf(roleArg)!!
    when (role) {
        Role.SENDER -> {
            val host = NetworkHostAndPort("localhost", 10006)
            println("Connecting to sender node ($host)")
            CordaRPCClient(host).start("demo", "demo").use {
                sender(it.proxy)
            }
        }
        Role.RECIPIENT -> {
            val host = NetworkHostAndPort("localhost", 10009)
            println("Connecting to the recipient node ($host)")
            CordaRPCClient(host).start("demo", "demo").use {
                recipient(it.proxy)
            }
        }
    }
}

/** An in memory test zip attachment of at least numOfClearBytes size, will be used. */
fun sender(rpc: CordaRPCOps, numOfClearBytes: Int = 1024) { // default size 1K.
    val (inputStream, hash) = InputStreamAndHash.createInMemoryTestZip(numOfClearBytes, 0)
    sender(rpc, inputStream, hash)
}

fun sender(rpc: CordaRPCOps, inputStream: InputStream, hash: SecureHash.SHA256) {
    // Get the identity key of the other side (the recipient).
    val executor = Executors.newScheduledThreadPool(1)
    val otherSide: Party = poll(executor, DUMMY_BANK_B.name.toString()) { rpc.partyFromX500Name(DUMMY_BANK_B.name) }.get()

    // Make sure we have the file in storage
    if (!rpc.attachmentExists(hash)) {
        inputStream.use {
            val id = rpc.uploadAttachment(it)
            require(hash == id) { "Id was '$id' instead of '$hash'" }
        }
        require(rpc.attachmentExists(hash))
    }

    val flowHandle = rpc.startTrackedFlow(::AttachmentDemoFlow, otherSide, hash)
    flowHandle.progress.subscribe(::println)
    val stx = flowHandle.returnValue.getOrThrow()
    println("Sent ${stx.id}")
}

@StartableByRPC
class AttachmentDemoFlow(val otherSide: Party, val hash: SecureHash.SHA256) : FlowLogic<SignedTransaction>() {

    object SIGNING : ProgressTracker.Step("Signing transaction")

    override val progressTracker: ProgressTracker = ProgressTracker(SIGNING)

    @Suspendable
    override fun call(): SignedTransaction {
        // Create a trivial transaction with an output that describes the attachment, and the attachment itself
        val ptx = TransactionType.General.Builder(notary = DUMMY_NOTARY)
        ptx.addOutputState(AttachmentContract.State(hash))
        ptx.addAttachment(hash)

        progressTracker.currentStep = SIGNING

        // Send the transaction to the other recipient
        val stx = serviceHub.signInitialTransaction(ptx)

        return subFlow(FinalityFlow(stx, setOf(otherSide))).single()
    }
}

fun recipient(rpc: CordaRPCOps) {
    println("Waiting to receive transaction ...")
    val stx = rpc.verifiedTransactions().second.toBlocking().first()
    val wtx = stx.tx
    if (wtx.attachments.isNotEmpty()) {
        if (wtx.outputs.isNotEmpty()) {
            val state = wtx.outputsOfType<AttachmentContract.State>().single()
            require(rpc.attachmentExists(state.hash))

            // Download the attachment via the Web endpoint.
            val connection = URL("http://localhost:10010/attachments/${state.hash}").openConnection() as HttpURLConnection
            try {
                require(connection.responseCode == SC_OK) { "HTTP status code was ${connection.responseCode}" }
                require(connection.contentType == APPLICATION_OCTET_STREAM) { "Content-Type header was ${connection.contentType}" }
                require(connection.contentLength > 1024) { "Attachment contains only ${connection.contentLength} bytes" }
                require(connection.getHeaderField(CONTENT_DISPOSITION) == "attachment; filename=\"${state.hash}.zip\"") {
                    "Content-Disposition header was ${connection.getHeaderField(CONTENT_DISPOSITION)}"
                }

                // Write out the entries inside this jar.
                println("Attachment JAR contains these entries:")
                JarInputStream(connection.inputStream).use { it ->
                    while (true) {
                        val e = it.nextJarEntry ?: break
                        println("Entry> ${e.name}")
                        it.closeEntry()
                    }
                }
            } finally {
                connection.disconnect()
            }
            println("File received - we're happy!\n\nFinal transaction is:\n\n${Emoji.renderIfSupported(wtx)}")
        } else {
            println("Error: no output state found in ${wtx.id}")
        }
    } else {
        println("Error: no attachments found in ${wtx.id}")
    }
}

private fun printHelp(parser: OptionParser) {
    println("""
    Usage: attachment-demo --role [RECIPIENT|SENDER] [options]
    Please refer to the documentation in docs/build/index.html for more info.

    """.trimIndent())
    parser.printHelpOn(System.out)
}

class AttachmentContract : Contract {
    override val legalContractReference: SecureHash
        get() = SecureHash.zeroHash // TODO not implemented

    override fun verify(tx: LedgerTransaction) {
        val state = tx.outputsOfType<AttachmentContract.State>().single()
        val attachment = tx.attachments.single()
        require(state.hash == attachment.id)
    }

    data class State(val hash: SecureHash.SHA256) : ContractState {
        override val contract: Contract = AttachmentContract()
        override val participants: List<AbstractParty> = emptyList()
    }
}
