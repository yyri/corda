package net.corda.node.services.persistence

import com.google.common.annotations.VisibleForTesting
import net.corda.core.bufferUntilSubscribed
import net.corda.core.crypto.SecureHash
import net.corda.core.messaging.DataFeed
import net.corda.core.schemas.MappedSchema
import net.corda.core.serialization.*
import net.corda.core.transactions.SignedTransaction
import net.corda.node.services.api.WritableTransactionStorage
import net.corda.node.utilities.*
import rx.Observable
import rx.subjects.PublishSubject
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.persistence.*
import kotlin.concurrent.read
import kotlin.concurrent.write

fun <T: Any> deserializeFromByteArray(blob: ByteArray): T =  SerializedBytes<T>(blob, true).deserialize()

fun <T: Any> serializeToByteArray(v: T): ByteArray = v.serialize(storageKryo(), true).bytes

object TransactionSchema

object TransactionSchemaV1 : MappedSchema(schemaFamily = TransactionSchema.javaClass, version = 1,
        mappedTypes = listOf(Transaction::class.java)) {

    @Entity
    @Table(name = "${NODE_DATABASE_PREFIX}transactions")
    class Transaction(
            @Id
            @Column
            var tx_id: String = "",

            @Lob
            @Column
            var transaction: ByteArray = ByteArray(0)
    )
}

class DBTransactionStorage : WritableTransactionStorage, SingletonSerializeAsToken() {

    private companion object {
        fun createTransactionsMap(): AppendOnlyPersistentMap<SecureHash, SignedTransaction, TransactionSchemaV1.Transaction, String> {
            return AppendOnlyPersistentMap(
                    cacheBound = 1024,
                    toPersistentEntityKey = { it.toString() },
                    fromPersistentEntity = { Pair(SecureHash.parse(it.tx_id), deserializeFromByteArray<SignedTransaction>(it.transaction)) },
                    toPersistentEntity = { key: SecureHash, value: SignedTransaction ->
                        TransactionSchemaV1.Transaction().apply {
                            tx_id = key.toString()
                            transaction = serializeToByteArray(value)
                        }
                    },
                    persistentEntityClass = TransactionSchemaV1.Transaction::class.java
            )
        }
    }
    private val txStorage = createTransactionsMap()
    private val lock = ReentrantReadWriteLock()

    override fun addTransaction(transaction: SignedTransaction): Boolean =
        lock.write {
            txStorage[transaction.id] = transaction
            updatesPublisher.bufferUntilDatabaseCommit().onNext(transaction)
            return true
        }

    override fun getTransaction(id: SecureHash): SignedTransaction? =
        lock.read {
            return txStorage[id]
        }

    private val updatesPublisher = PublishSubject.create<SignedTransaction>().toSerialized()
    override val updates: Observable<SignedTransaction> = updatesPublisher.wrapWithDatabaseTransaction()

    override fun track(): DataFeed<List<SignedTransaction>, SignedTransaction> =
        lock.read {
            return DataFeed(txStorage.loadAll().map { it.second }.toList(), updatesPublisher.bufferUntilSubscribed().wrapWithDatabaseTransaction())
        }

    @VisibleForTesting
    val transactions: Iterable<SignedTransaction> get() = synchronized(this) { txStorage.loadAll().map { it.second }.toList() }
}
