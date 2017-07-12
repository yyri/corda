package net.corda.traderdemo

import com.google.common.util.concurrent.Futures
import net.corda.client.rpc.notUsed
import net.corda.contracts.CommercialPaper
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.*
import net.corda.core.getOrThrow
import net.corda.core.internal.Emoji
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.node.services.vault.builder
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.loggerFor
import net.corda.flows.IssuerFlow.IssuanceRequester
import net.corda.schemas.CashSchemaV1
import net.corda.testing.BOC
import net.corda.testing.DUMMY_NOTARY
import net.corda.testing.contracts.calculateRandomlySizedAmounts
import net.corda.traderdemo.flow.SellerFlow
import org.bouncycastle.asn1.x500.X500Name
import java.util.*

/**
 * Interface for communicating with nodes running the trader demo.
 */
class TraderDemoClientApi(val rpc: CordaRPCOps) {
    private companion object {
        val logger = loggerFor<TraderDemoClientApi>()
    }

    val cashCount: Int get() {
        val (vault, vaultUpdates) = rpc.vaultAndUpdates()
        vaultUpdates.notUsed()
        return vault.filterStatesOfType<Cash.State>().size
    }


    private fun getBalance(currency: Currency): Amount<Currency> {
        val sum = builder { CashSchemaV1.PersistentCashState::pennies.sum(groupByColumns = listOf(CashSchemaV1.PersistentCashState::currency)) }
        val sumCriteria = QueryCriteria.VaultCustomQueryCriteria(sum)

        val ccyIndex = builder { CashSchemaV1.PersistentCashState::currency.equal(currency.currencyCode) }
        val ccyCriteria = QueryCriteria.VaultCustomQueryCriteria(ccyIndex)

        val results = rpc.vaultQueryByCriteria(sumCriteria.and(ccyCriteria), FungibleAsset::class.java)
        if (results.otherResults.isEmpty()) {
            return Amount(0L, currency)
        } else {
            @Suppress("UNCHECKED_CAST")
            val quantity = results.otherResults[0] as Long
            return Amount(quantity, currency)
        }
    }


    val dollarCashBalance: Amount<Currency> get() = getBalance(USD)

    val commercialPaperCount: Int get() {
        val (vault, vaultUpdates) = rpc.vaultAndUpdates()
        vaultUpdates.notUsed()
        return vault.filterStatesOfType<CommercialPaper.State>().size
    }

    fun runBuyer(amount: Amount<Currency> = 30000.DOLLARS, anonymous: Boolean = true) {
        val bankOfCordaParty = rpc.partyFromX500Name(BOC.name)
                ?: throw IllegalStateException("Unable to locate ${BOC.name} in Network Map Service")
        val notaryLegalIdentity = rpc.partyFromX500Name(DUMMY_NOTARY.name)
                ?: throw IllegalStateException("Unable to locate ${DUMMY_NOTARY.name} in Network Map Service")
        val notaryNode = rpc.nodeIdentityFromParty(notaryLegalIdentity)
                ?: throw IllegalStateException("Unable to locate notary node in network map cache")
        val me = rpc.nodeIdentity()
        val amounts = calculateRandomlySizedAmounts(amount, 3, 10, Random())
        // issuer random amounts of currency totaling 30000.DOLLARS in parallel
        val resultFutures = amounts.map { pennies ->
            rpc.startFlow(::IssuanceRequester, Amount(pennies, amount.token), me.legalIdentity, OpaqueBytes.of(1), bankOfCordaParty, notaryNode.notaryIdentity, anonymous).returnValue
        }

        Futures.allAsList(resultFutures).getOrThrow()
    }

    fun runSeller(amount: Amount<Currency> = 1000.0.DOLLARS, counterparty: X500Name) {
        val otherParty = rpc.partyFromX500Name(counterparty) ?: throw IllegalStateException("Don't know $counterparty")
        // The seller will sell some commercial paper to the buyer, who will pay with (self issued) cash.
        //
        // The CP sale transaction comes with a prospectus PDF, which will tag along for the ride in an
        // attachment. Make sure we have the transaction prospectus attachment loaded into our store.
        //
        // This can also be done via an HTTP upload, but here we short-circuit and do it from code.
        if (!rpc.attachmentExists(SellerFlow.PROSPECTUS_HASH)) {
            javaClass.classLoader.getResourceAsStream("bank-of-london-cp.jar").use {
                val id = rpc.uploadAttachment(it)
                check(SellerFlow.PROSPECTUS_HASH == id)
            }
        }

        // The line below blocks and waits for the future to resolve.
        val stx = rpc.startFlow(::SellerFlow, otherParty, amount).returnValue.getOrThrow()
        println("Sale completed - we have a happy customer!\n\nFinal transaction is:\n\n${Emoji.renderIfSupported(stx.tx)}")
    }
}
