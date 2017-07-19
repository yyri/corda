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
import javax.persistence.*


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

fun <T : Any> deserializeFromByteArray(blob: ByteArray): T =  SerializedBytes<T>(blob, true).deserialize()

fun <T: Any> serializeToByteArray(v: T): ByteArray = v.serialize(storageKryo(), true).bytes

class DBTransactionStorage : WritableTransactionStorage, SingletonSerializeAsToken() {

    override fun addTransaction(transaction: SignedTransaction): Boolean =
        synchronized(this) {
            val tr: TransactionSchemaV1.Transaction = TransactionSchemaV1.Transaction()
            tr.tx_id = transaction.id.toString()
            tr.transaction = serializeToByteArray(transaction)
            val session = DatabaseTransactionManager.current().session
            try {
                session.saveOrUpdate(tr)
                updatesPublisher.bufferUntilDatabaseCommit().onNext(transaction)
                true
            } catch (e : EntityExistsException) {
                //exposedLogger.warn("Duplicate recording of transaction ${transaction.id}")
                false
            } catch (e: Throwable) {
                false
            }
        }


    override fun getTransaction(id: SecureHash): SignedTransaction? {
        synchronized(this) {
            val session = DatabaseTransactionManager.current().session
            val result = session.find(TransactionSchemaV1.Transaction::class.java, id.toString())
            return if(result != null) deserializeFromByteArray<SignedTransaction>(result.transaction) else null
        }
    }

    private val updatesPublisher = PublishSubject.create<SignedTransaction>().toSerialized()
    override val updates: Observable<SignedTransaction> = updatesPublisher.wrapWithDatabaseTransaction()

    override fun track(): DataFeed<List<SignedTransaction>, SignedTransaction> =
        synchronized(this) {
            return DataFeed(getAll(), updatesPublisher.bufferUntilSubscribed().wrapWithDatabaseTransaction())
        }

    @VisibleForTesting
    val transactions: Iterable<SignedTransaction> get() = synchronized(this) { getAll() }

    fun getAll() : List<SignedTransaction> {
        val session = DatabaseTransactionManager.current().session
        val query = session.createQuery("FROM " + TransactionSchemaV1.Transaction::class.java.name )
        val res: MutableList<TransactionSchemaV1.Transaction> = query.resultList as MutableList<TransactionSchemaV1.Transaction>
        return res.map { deserializeFromByteArray<SignedTransaction>(it.transaction) }
    }
}
