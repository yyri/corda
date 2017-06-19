package net.corda.core.transactions;

import net.corda.core.contracts.Command;
import net.corda.core.contracts.ContractState;
import net.corda.core.contracts.StateAndRef;
import net.corda.core.contracts.TimeWindow;
import net.corda.core.crypto.SecureHash;
import net.corda.core.identity.Party;

import java.util.List;

public interface NewTransaction {
    enum TransactionStatus {
        INCOMPLETE,
        REQUIRES_SIGNATURES,
        REQUIRES_NOTARY_SIGNATURE,
        FULLY_SIGNED
    }

    TimeWindow getTime();

    Party getNotary();

    List<StateAndRef<ContractState>> getInputs();

    List<ContractState> getOutputs();

    List<Command> getCommands();

    List<SecureHash> getAttachments();

    TransactionView getView();

    TransactionStatus getStatus();
}
