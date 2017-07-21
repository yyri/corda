package net.corda.core.transactions

import net.corda.core.contracts.*
import net.corda.core.identity.Party
import net.corda.core.internal.indexOfOrThrow
import java.security.PublicKey
import java.util.*
import java.util.function.Predicate

/**
 * An abstract class defining fields shared by all transaction types in the system.
 */
abstract class BaseTransaction(
        /** The inputs of this transaction. Note that in BaseTransaction subclasses the type of this list may change! */
        open val inputs: List<*>,
        /** Ordered list of states defined by this transaction, along with the associated notaries. */
        val outputs: List<TransactionState<ContractState>>,
        /**
         * If present, the notary for this transaction. If absent then the transaction is not notarised at all.
         * This is intended for issuance/genesis transactions that don't consume any other states and thus can't
         * double spend anything.
         */
        val notary: Party?,
        /**
         * Public keys that need to be fulfilled by signatures in order for the transaction to be valid.
         * In a [SignedTransaction] this list is used to check whether there are any missing signatures. Note that
         * there is nothing that forces the list to be the _correct_ list of signers for this transaction until
         * the transaction is verified by using [LedgerTransaction.verify].
         *
         * It includes the notary key, if the notary field is set.
         */
        val mustSign: List<PublicKey>,
        /**
         * Pointer to a class that defines the behaviour of this transaction: either normal, or "notary changing".
         */
        val type: TransactionType,
        /**
         * If specified, a time window in which this transaction may have been notarised. Contracts can check this
         * time window to find out when a transaction is deemed to have occurred, from the ledger's perspective.
         */
        val timeWindow: TimeWindow?
) : NamedByHash {

    protected fun checkInvariants() {
        if (notary == null) check(inputs.isEmpty()) { "The notary must be specified explicitly for any transaction that has inputs" }
        if (timeWindow != null) check(notary != null) { "If a time-window is provided, there must be a notary" }
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        return other is BaseTransaction &&
                notary == other.notary &&
                mustSign == other.mustSign &&
                type == other.type &&
                timeWindow == other.timeWindow
    }

    override fun hashCode() = Objects.hash(notary, mustSign, type, timeWindow)

    override fun toString(): String = "${javaClass.simpleName}(id=$id)"

    /**
     * Returns a [StateAndRef] for the given output index.
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : ContractState> outRef(index: Int): StateAndRef<T> = StateAndRef(outputs[index] as TransactionState<T>, StateRef(id, index))


    /**
     * Returns a [StateAndRef] for the requested output state, or throws [IllegalArgumentException] if not found.
     */
    fun <T : ContractState> outRef(state: ContractState): StateAndRef<T> = outRef(outputStates.indexOfOrThrow(state))

    /**
     * Helper property to return a list of [ContractState] objects, rather than the often less convenient [TransactionState]
     */
    val outputStates: List<ContractState> get() = outputs.map { it.data }

    /**
     * Helper to simplify getting an indexed output.
     * @param index the position of the item in the output.
     * @return The ContractState at the requested index
     */
    fun getOutput(index: Int): ContractState = outputs[index].data

    /**
     * Helper to simplify getting all output states of a particular class, interface, or base class.
     * @param clazz The class type used for filtering via an [Class.isInstance] check.
     * Clazz must be an extension of [ContractState].
     * @return the possibly empty list of output states matching the clazz restriction.
     */
    fun <T : ContractState> outputsOfType(clazz: Class<T>): List<T> {
        @Suppress("UNCHECKED_CAST")
        return outputs.filter { clazz.isInstance(it.data) }.map { it.data as T }
    }

    /**
     * Helper to simplify filtering outputs according to a [Predicate].
     * @param predicate A filtering function taking a state of type T and returning true if it should be included in the list.
     * The class filtering is applied before the predicate.
     * @param clazz The class type used for filtering via an [Class.isInstance] check.
     * Clazz must be an extension of [ContractState].
     * @return the possibly empty list of output states matching the predicate and clazz restrictions.
     */
    fun <T : ContractState> filterOutputs(predicate: Predicate<T>, clazz: Class<T>): List<T> {
        return outputsOfType(clazz).filter { predicate.test(it) }
    }

    /**
     * Helper to simplify finding a single output matching a [Predicate].
     * @param predicate A filtering function taking a state of type T and returning true if this is the desired item.
     * The class filtering is applied before the predicate.
     * @param clazz The class type used for filtering via an [Class.isInstance] check.
     * Clazz must be an extension of [ContractState].
     * @return the single item matching the predicate.
     * @throws IllegalArgumentException if no item, or multiple items are found matching the requirements.
     */
    fun <T : ContractState> findOutput(predicate: Predicate<T>, clazz: Class<T>): T {
        return filterOutputs(predicate, clazz).single()
    }

    /**
     * Helper to simplify getting all output [StateAndRef] items of a particular state class, interface, or base class.
     * @param clazz The class type used for filtering via an [Class.isInstance] check.
     * Clazz must be an extension of [ContractState].
     * @return the possibly empty list of output [StateAndRef<T>] states matching the clazz restriction.
     */
    fun <T : ContractState> outRefsOfType(clazz: Class<T>): List<StateAndRef<T>> {
        @Suppress("UNCHECKED_CAST")
        return outputs.mapIndexed { index, state -> StateAndRef(state, StateRef(id, index)) }
                .filter { clazz.isInstance(it.state.data) }
                .map { it as StateAndRef<T> }
    }

    /**
     * Helper to simplify filtering output [StateAndRef] items according to a [Predicate].
     * @param predicate A filtering function taking a state of type T and returning true if it should be included in the list.
     * The class filtering is applied before the predicate.
     * @param clazz The class type used for filtering via an [Class.isInstance] check.
     * Clazz must be an extension of [ContractState].
     * @return the possibly empty list of output [StateAndRef] states matching the predicate and clazz restrictions.
     */
    fun <T : ContractState> filterOutRefs(predicate: Predicate<T>, clazz: Class<T>): List<StateAndRef<T>> {
        return outRefsOfType(clazz).filter { predicate.test(it.state.data) }
    }

    /**
     * Helper to simplify finding a single output [StateAndRef] matching a [Predicate].
     * @param predicate A filtering function taking a state of type T and returning true if this is the desired item.
     * The class filtering is applied before the predicate.
     * @param clazz The class type used for filtering via an [Class.isInstance] check.
     * Clazz must be an extension of [ContractState].
     * @return the single [StateAndRef] item matching the predicate.
     * @throws IllegalArgumentException if no item, or multiple items are found matching the requirements.
     */
    fun <T : ContractState> findOutRef(predicate: Predicate<T>, clazz: Class<T>): StateAndRef<T> {
        return filterOutRefs(predicate, clazz).single()
    }

    //Kotlin extension methods to take advantage of Kotlin's smart type inference when querying the LedgerTransaction
    inline fun <reified T : ContractState> outputsOfType(): List<T> = this.outputsOfType(T::class.java)

    inline fun <reified T : ContractState> filterOutputs(crossinline predicate: (T) -> Boolean): List<T> {
        return filterOutputs(Predicate { predicate(it) }, T::class.java)
    }

    inline fun <reified T : ContractState> findOutput(crossinline predicate: (T) -> Boolean): T {
        return findOutput(Predicate { predicate(it) }, T::class.java)
    }

    inline fun <reified T : ContractState> outRefsOfType(): List<StateAndRef<T>> = this.outRefsOfType(T::class.java)

    inline fun <reified T : ContractState> filterOutRefs(crossinline predicate: (T) -> Boolean): List<StateAndRef<T>> {
        return filterOutRefs(Predicate { predicate(it) }, T::class.java)
    }

    inline fun <reified T : ContractState> findOutRef(crossinline predicate: (T) -> Boolean): StateAndRef<T> {
        return findOutRef(Predicate { predicate(it) }, T::class.java)
    }
}