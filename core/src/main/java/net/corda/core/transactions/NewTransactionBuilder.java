package net.corda.core.transactions;

import net.corda.core.contracts.*;
import net.corda.core.crypto.SecureHash;
import net.corda.core.flows.FlowException;
import net.corda.core.identity.Party;
import net.corda.core.node.ServiceHub;
import net.corda.core.serialization.CordaSerializable;
import net.corda.core.utilities.UntrustworthyData;

import javax.annotation.Nonnull;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@CordaSerializable
public final class NewTransactionBuilder implements NewTransaction {
    private final List<StateAndRef<ContractState>> inputs = new ArrayList<>();
    private final List<ContractState> outputs = new ArrayList<>();
    private final List<Command> commands = new ArrayList<>();
    private final List<SecureHash> attachments = new ArrayList<>();
    private final Party notary;
    private TimeWindow timeWindow;

    public NewTransactionBuilder(Party notary) {
        this.notary = notary;
    }

    @Override
    public TimeWindow getTime() {
        return timeWindow;
    }

    public NewTransactionBuilder setTime(@Nonnull TimeWindow timeWindow) {
        if (this.timeWindow != null) {
            throw new IllegalArgumentException("TimeWindow already set");
        }
        this.timeWindow = timeWindow;
        return this;
    }

    @Override
    public Party getNotary() {
        return notary;
    }

    @Override
    public List<StateAndRef<ContractState>> getInputs() {
        return Collections.unmodifiableList(inputs);
    }

    @Override
    public List<ContractState> getOutputs() {
        return Collections.unmodifiableList(outputs);
    }

    @Override
    public List<Command> getCommands() {
        return Collections.unmodifiableList(commands);
    }

    @Override
    public List<SecureHash> getAttachments() {
        return Collections.unmodifiableList(attachments);
    }

    @Override
    public TransactionView getView() {
        return new TransactionViewImpl(this);
    }

    @Override
    public TransactionStatus getStatus() {
        return TransactionStatus.INCOMPLETE;
    }

    public NewTransactionBuilder addInputState(@Nonnull StateAndRef<ContractState> stateAndRef) {
        Party notary = stateAndRef.getState().getNotary();
        if (!Objects.equals(notary, this.notary)) {
            String msg = String.format("Input state requires notary \"%s\" " +
                            "which does not match the transaction notary \"%s\".",
                    notary,
                    this.notary);
            throw new IllegalArgumentException(msg);
        }
        inputs.add(stateAndRef);
        return this;
    }

    public NewTransactionBuilder addAttachment(@Nonnull SecureHash attachment) {
        attachments.add(attachment);
        return this;
    }

    public NewTransactionBuilder addOutputState(@Nonnull ContractState state) {
        outputs.add(state);
        return this;
    }

    public NewTransactionBuilder addCommand(@Nonnull Command command) {
        commands.add(command);
        return this;
    }

    private <T> boolean checkList(List<T> newList, List<T> oldList) {
        // Check that any changes are appended at the end, not modifications of our original data.
        return (newList.size() >= oldList.size() &&
                oldList.equals(newList.subList(0, oldList.size())));
    }

    public NewTransactionBuilder merge(@Nonnull UntrustworthyData<NewTransactionBuilder> receivedBuilder) throws FlowException {
        return receivedBuilder.unwrap(data -> {
            if (!Objects.equals(notary, data.notary)) {
                throw new FlowException("Notary in received data does not match");
            }
            if (timeWindow == null || timeWindow.equals(data.timeWindow)) {
                throw new FlowException("Notary in received data does not match");
            }
            if (checkList(data.inputs, inputs)) {
                throw new FlowException("Other party can only add inputs, not modify existing inputs");
            }
            if (checkList(data.outputs, outputs)) {
                throw new FlowException("Other party can only add outputs, not modify existing outputs");
            }
            if (checkList(data.attachments, attachments)) {
                throw new FlowException("Other party can only add attachments, not modify existing attachments");
            }
            if (checkList(data.commands, commands)) {
                throw new FlowException("Other party can only add commands, not modify existing commands");
            }

            return data;
        });
    }

    public NewTransactionImpl build(@Nonnull ServiceHub serviceHub) throws InvalidKeyException, SignatureException, AttachmentResolutionException, TransactionResolutionException, TransactionVerificationException {
        return build(serviceHub, serviceHub.getLegalIdentityKey());
    }

    public NewTransactionImpl build(@Nonnull ServiceHub serviceHub, @Nonnull PublicKey identity) throws InvalidKeyException, SignatureException, AttachmentResolutionException, TransactionResolutionException, TransactionVerificationException {
        return new NewTransactionImpl(this, serviceHub, identity);
    }
}
