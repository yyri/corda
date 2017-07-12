package net.corda.traderdemo.flow

import co.paralleluniverse.fibers.Suspendable
import net.corda.contracts.CommercialPaper
import net.corda.core.contracts.Amount
import net.corda.core.contracts.FungibleAsset
import net.corda.core.contracts.TransactionGraphSearch
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatedBy
import net.corda.core.identity.Party
import net.corda.core.internal.Emoji
import net.corda.core.node.NodeInfo
import net.corda.core.node.services.queryBy
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.node.services.vault.Sort
import net.corda.core.node.services.vault.builder
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap
import net.corda.flows.TwoPartyTradeFlow
import net.corda.schemas.CashSchemaV1
import java.util.*

@InitiatedBy(SellerFlow::class)
class BuyerFlow(val otherParty: Party) : FlowLogic<Unit>() {

    object STARTING_BUY : ProgressTracker.Step("Seller connected, purchasing commercial paper asset")

    override val progressTracker: ProgressTracker = ProgressTracker(STARTING_BUY)

    @Suspendable
    override fun call() {
        progressTracker.currentStep = STARTING_BUY

        // Receive the offered amount and automatically agree to it (in reality this would be a longer negotiation)
        val amount = receive<Amount<Currency>>(otherParty).unwrap { it }
        val notary: NodeInfo = serviceHub.networkMapCache.notaryNodes[0]
        val buyer = TwoPartyTradeFlow.Buyer(
                otherParty,
                notary.notaryIdentity,
                amount,
                CommercialPaper.State::class.java)

        // This invokes the trading flow and out pops our finished transaction.
        val tradeTX: SignedTransaction = subFlow(buyer)

        println("Purchase complete - we are a happy customer! Final transaction is: " +
                "\n\n${Emoji.renderIfSupported(tradeTX.tx)}")

        logIssuanceAttachment(tradeTX)
        logBalance()
    }

    private fun logBalance() {
        val sum = builder {
            CashSchemaV1.PersistentCashState::pennies.sum(groupByColumns = listOf(CashSchemaV1.PersistentCashState::issuerParty,
                    CashSchemaV1.PersistentCashState::currency),
                    orderBy = Sort.Direction.DESC)
        }

        val sums = serviceHub.vaultQueryService.queryBy<FungibleAsset<*>>(QueryCriteria.VaultCustomQueryCriteria(sum)).otherResults
        val balances = mutableListOf<Amount<Currency>>()
        for (index in 0..sums.size - 1 step 2) {
            balances += Amount(sums[index] as Long, sums[index + 1] as Currency)
        }

        println("Remaining balance: ${balances.joinToString()}")
    }

    private fun logIssuanceAttachment(tradeTX: SignedTransaction) {
        // Find the original CP issuance.
        val search = TransactionGraphSearch(serviceHub.validatedTransactions, listOf(tradeTX.tx))
        search.query = TransactionGraphSearch.Query(withCommandOfType = CommercialPaper.Commands.Issue::class.java,
                followInputsOfType = CommercialPaper.State::class.java)
        val cpIssuance = search.call().single()

        // Buyer will fetch the attachment from the seller automatically when it resolves the transaction.

        cpIssuance.attachments.first().let {
            println("""

The issuance of the commercial paper came with an attachment. You can find it in the attachments directory: $it.jar

${Emoji.renderIfSupported(cpIssuance)}""")
        }
    }
}
