package net.corda.core.transactions;

import net.corda.core.contracts.*;
import net.corda.core.crypto.DigitalSignature;
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
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@CordaSerializable
public final class NewTransactionImpl implements NewTransaction {
    private final List<StateAndRef<ContractState>> inputs;
    private final WireTransaction wireTx;
    private final List<DigitalSignature.WithKey> signatures = new ArrayList<>();
    private final SecureHash id;

    NewTransactionImpl(@Nonnull NewTransaction transaction, @Nonnull ServiceHub serviceHub, @Nonnull PublicKey identity) throws InvalidKeyException, SignatureException, AttachmentResolutionException, TransactionResolutionException, TransactionVerificationException {
        inputs = transaction.getInputs();
        List<ContractState> outputs = transaction.getOutputs();
        List<SecureHash> attachments = transaction.getAttachments();
        List<Command> commands = transaction.getCommands();
        Party notary = transaction.getNotary();
        for (StateAndRef<ContractState> input : inputs) {
            if (input.getState().getNotary() != notary) {
                throw new IllegalArgumentException("Inputs not using correct notary");
            }
        }
        List<StateRef> inputRefs = inputs.stream().map(StateAndRef<ContractState>::getRef).collect(Collectors.toList());
        List<TransactionState<?>> outputStates = outputs.stream().map(it -> new TransactionState<>(it, notary, null)).collect(Collectors.toList());
        Set<PublicKey> signers = commands.stream().flatMap(it -> it.getSigners().stream()).collect(Collectors.toSet());
        if (!inputs.isEmpty()) {
            signers.add(notary.getOwningKey());
        }
        wireTx = new WireTransaction(inputRefs,
                attachments,
                outputStates,
                commands,
                notary,
                new ArrayList<>(signers),
                TransactionType.General.INSTANCE,
                transaction.getTime());
        id = wireTx.getId();
        DigitalSignature.WithKey initialSignature = serviceHub.getKeyManagementService().sign(id.getBytes(), identity);
        signatures.add(initialSignature);
        verifyTransaction(serviceHub);
    }

    public SecureHash getId() {
        return id;
    }

    @Override
    public TimeWindow getTime() {
        return wireTx.getTimeWindow();
    }

    @Override
    public Party getNotary() {
        return wireTx.getNotary();
    }

    @Override
    public List<StateAndRef<ContractState>> getInputs() {
        return inputs;
    }

    @Override
    public List<ContractState> getOutputs() {
        return wireTx.getOutputs().stream().map(TransactionState<ContractState>::getData).collect(Collectors.toList());
    }

    @Override
    public List<Command> getCommands() {
        return wireTx.getCommands();
    }

    @Override
    public List<SecureHash> getAttachments() {
        return wireTx.getAttachments();
    }

    @Override
    public TransactionView getView() {
        return new TransactionViewImpl(this);
    }

    @Override
    public TransactionStatus getStatus() {
        List<PublicKey> missing = getMissingSignatures();
        if (missing.isEmpty()) {
            return TransactionStatus.FULLY_SIGNED;
        } else if (missing.size() == 1 && getNotary().getOwningKey().equals(missing.get(0))) {
            return TransactionStatus.REQUIRES_NOTARY_SIGNATURE;
        }
        return TransactionStatus.REQUIRES_SIGNATURES;
    }

    public List<PublicKey> getRequiredSigners() {
        return wireTx.getMustSign();
    }

    public List<PublicKey> getMissingSignatures() {
        Set<PublicKey> required = new HashSet<>(wireTx.getMustSign());
        required.removeAll(signatures.stream().map(DigitalSignature.WithKey::getBy).collect(Collectors.toList()));
        return new ArrayList<>(required);
    }

    public void checkSignatures() throws InvalidKeyException, SignatureException {
        for (DigitalSignature.WithKey sig : signatures) {
            sig.verify(id.getBytes());
        }
    }

    public void verifySignatures() throws InvalidKeyException, SignatureException {
        verifySignaturesExcept(new ArrayList<>());
    }

    public void verifySignaturesExcept(@Nonnull PublicKey... excluded) throws InvalidKeyException, SignatureException {
        verifySignaturesExcept(Arrays.asList(excluded));
    }

    public void verifySignaturesExcept(@Nonnull List<PublicKey> excluded) throws InvalidKeyException, SignatureException {
        checkSignatures();
        Set<PublicKey> required = new HashSet<>(wireTx.getMustSign());
        required.removeAll(signatures.stream().map(DigitalSignature.WithKey::getBy).collect(Collectors.toList()));
        required.removeAll(excluded);
        if (!required.isEmpty()) {
            throw new SignatureException("Signatures missing: " + required);
        }
    }

    public void verifyTransaction(@Nonnull ServiceHub serviceHub) throws InvalidKeyException, SignatureException, AttachmentResolutionException, TransactionResolutionException, TransactionVerificationException {
        checkSignatures();
        wireTx.toLedgerTransaction(serviceHub).verify();
    }

    public NewTransactionImpl addSignature(@Nonnull DigitalSignature.WithKey newSig) throws InvalidKeyException, SignatureException {
        newSig.verify(id.getBytes());
        signatures.add(newSig);
        return this;
    }

    public FilteredTransaction buildFilteredTransaction(Predicate<Object> filter) {
        return FilteredTransaction.Companion.buildMerkleTransaction(wireTx, filter);
    }

    public NewTransactionImpl merge(@Nonnull UntrustworthyData<NewTransactionImpl> receivedTransaction) throws FlowException {
        return receivedTransaction.unwrap(tx -> {
            List<DigitalSignature.WithKey> receivedSignatures = tx.signatures;
            for (DigitalSignature.WithKey sig : receivedSignatures) {
                try {
                    sig.verify(id.getBytes()); // verify they signed our transaction, not something else
                } catch (InvalidKeyException | SignatureException sigEx) {
                    throw new FlowException(sigEx);
                }
            }
            if (receivedSignatures.size() < signatures.size()
                    || !signatures.equals(receivedSignatures.subList(0, signatures.size()))) {
                throw new FlowException("Other party can only extend signatures, not modify existing ones");
            }
            // extend our list and return
            signatures.addAll(receivedSignatures.subList(signatures.size(), receivedSignatures.size()));
            return this;
        });
    }
}
