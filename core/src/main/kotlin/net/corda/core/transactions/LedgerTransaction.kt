package net.corda.core.transactions

import net.corda.core.contracts.*
import net.corda.core.crypto.SecureHash
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import java.util.*

/**
 * A LedgerTransaction is derived from a [WireTransaction]. It is the result of doing the following operations:
 *
 * - Downloading and locally storing all the dependencies of the transaction.
 * - Resolving the input states and loading them into memory.
 * - Doing some basic key lookups on the [Command]s to see if any keys are from a recognised party, thus converting the
 *   [Command] objects into [AuthenticatedObject].
 * - Deserialising the output states.
 *
 * All the above refer to inputs using a (txhash, output index) pair.
 */
// TODO LedgerTransaction is not supposed to be serialisable as it references attachments, etc. The verification logic
// currently sends this across to out-of-process verifiers. We'll need to change that first.
// DOCSTART 1
@CordaSerializable
data class LedgerTransaction(
        /** The resolved input states which will be consumed/invalidated by the execution of this transaction. */
        override val inputs: List<StateAndRef<ContractState>>,
        override val outputs: List<TransactionState<ContractState>>,
        /** Arbitrary data passed to the program of each input state. */
        val commands: List<AuthenticatedObject<CommandData>>,
        /** A list of [Attachment] objects identified by the transaction that are needed for this transaction to verify. */
        val attachments: List<Attachment>,
        /** The hash of the original serialised WireTransaction. */
        override val id: SecureHash,
        override val notary: Party?,
        val timeWindow: TimeWindow?,
        val type: TransactionType
) : FullTransaction {
    //DOCEND 1
    init {
        check(notary != null || timeWindow == null) { "Transactions with time-windows must be notarised" }
        checkInputsHaveSameNotary()
        checkNoNotaryChange()
        checkEncumbrancesValid()
    }

    /**
     * Verifies this transaction and runs contract code. At this stage it is assumed that signatures have already been verified.
     *
     * @throws TransactionVerificationException if anything goes wrong.
     */
    @Throws(TransactionVerificationException::class)
    fun verify() = verifyContracts()

    /**
     * Check the transaction is contract-valid by running the verify() for each input and output state contract.
     * If any contract fails to verify, the whole transaction is considered to be invalid.
     */
    private fun verifyContracts() {
        val contracts = (inputs.map { it.state.data.contract } + outputs.map { it.data.contract }).toSet()
        for (contract in contracts) {
            try {
                contract.verify(this)
            } catch(e: Throwable) {
                throw TransactionVerificationException.ContractRejection(id, contract, e)
            }
        }
    }

    /**
     * Make sure the notary has stayed the same. As we can't tell how inputs and outputs connect, if there
     * are any inputs, all outputs must have the same notary.
     *
     * TODO: Is that the correct set of restrictions? May need to come back to this, see if we can be more
     *       flexible on output notaries.
     */
    private fun checkNoNotaryChange() {
        if (notary != null && inputs.isNotEmpty()) {
            outputs.forEach {
                if (it.notary != notary) {
                    throw TransactionVerificationException.NotaryChangeInWrongTransactionType(id, notary, it.notary)
                }
            }
        }
    }

    private fun checkEncumbrancesValid() {
        // Validate that all encumbrances exist within the set of input states.
        val encumberedInputs = inputs.filter { it.state.encumbrance != null }
        encumberedInputs.forEach { (state, ref) ->
            val encumbranceStateExists = inputs.any {
                it.ref.txhash == ref.txhash && it.ref.index == state.encumbrance
            }
            if (!encumbranceStateExists) {
                throw TransactionVerificationException.TransactionMissingEncumbranceException(
                        id,
                        state.encumbrance!!,
                        TransactionVerificationException.Direction.INPUT
                )
            }
        }

        // Check that, in the outputs, an encumbered state does not refer to itself as the encumbrance,
        // and that the number of outputs can contain the encumbrance.
        for ((i, output) in outputs.withIndex()) {
            val encumbranceIndex = output.encumbrance ?: continue
            if (encumbranceIndex == i || encumbranceIndex >= outputs.size) {
                throw TransactionVerificationException.TransactionMissingEncumbranceException(
                        id,
                        encumbranceIndex,
                        TransactionVerificationException.Direction.OUTPUT)
            }
        }
    }

    /**
     * Given a type and a function that returns a grouping key, associates inputs and outputs together so that they
     * can be processed as one. The grouping key is any arbitrary object that can act as a map key (so must implement
     * equals and hashCode).
     *
     * The purpose of this function is to simplify the writing of verification logic for transactions that may contain
     * similar but unrelated state evolutions which need to be checked independently. Consider a transaction that
     * simultaneously moves both dollars and euros (e.g. is an atomic FX trade). There may be multiple dollar inputs and
     * multiple dollar outputs, depending on things like how fragmented the owner's vault is and whether various privacy
     * techniques are in use. The quantity of dollars on the output side must sum to the same as on the input side, to
     * ensure no money is being lost track of. This summation and checking must be repeated independently for each
     * currency. To solve this, you would use groupStates with a type of Cash.State and a selector that returns the
     * currency field: the resulting list can then be iterated over to perform the per-currency calculation.
     */
    // DOCSTART 2
    fun <T : ContractState, K : Any> groupStates(ofType: Class<T>, selector: (T) -> K): List<InOutGroup<T, K>> {
        val inputs = inputs.map { it.state.data }.filterIsInstance(ofType)
        val outputs = outputs.map { it.data }.filterIsInstance(ofType)

        val inGroups: Map<K, List<T>> = inputs.groupBy(selector)
        val outGroups: Map<K, List<T>> = outputs.groupBy(selector)

        val result = ArrayList<InOutGroup<T, K>>()

        for ((k, v) in inGroups.entries)
            result.add(InOutGroup(v, outGroups[k] ?: emptyList(), k))
        for ((k, v) in outGroups.entries) {
            if (inGroups[k] == null)
                result.add(InOutGroup(emptyList(), v, k))
        }

        return result
    }
    // DOCEND 2

    /** See the documentation for the reflection-based version of [groupStates] */
    inline fun <reified T : ContractState, K : Any> groupStates(noinline selector: (T) -> K): List<InOutGroup<T, K>> {
        return groupStates(T::class.java, selector)
    }

    /** Utilities for contract writers to incorporate into their logic. */

    /**
     * A set of related inputs and outputs that are connected by some common attributes. An InOutGroup is calculated
     * using [groupStates] and is useful for handling cases where a transaction may contain similar but unrelated
     * state evolutions, for example, a transaction that moves cash in two different currencies. The numbers must add
     * up on both sides of the transaction, but the values must be summed independently per currency. Grouping can
     * be used to simplify this logic.
     */
    // DOCSTART 3
    data class InOutGroup<out T : ContractState, out K : Any>(val inputs: List<T>, val outputs: List<T>, val groupingKey: K)
    // DOCEND 3
}
