package net.corda.core.transactions;

import net.corda.core.contracts.*;
import net.corda.core.crypto.SecureHash;
import net.corda.core.identity.Party;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class TransactionViewImpl implements TransactionView {
    private final NewTransaction transaction;

    TransactionViewImpl(NewTransaction transaction) {
        this.transaction = transaction;
    }

    private <T> Stream<T> mapNotNullInputs(Function<StateAndRef<ContractState>, T> fun) {
        return transaction.getInputs().stream().map(fun).filter(Objects::nonNull);
    }

    @SuppressWarnings("unchecked")
    private <T extends ContractState> Stream<StateAndRef<T>> inputsOfTypeInternal(Class<T> clazz) {
        return mapNotNullInputs(it -> clazz.isAssignableFrom(it.getState().getData().getClass()) ? (StateAndRef<T>) it : null);
    }

    private <T> Stream<T> mapNotNullOutputs(Function<ContractState, T> fun) {
        return transaction.getOutputs().stream().map(fun).filter(Objects::nonNull);
    }

    @SuppressWarnings("unchecked")
    private <T extends ContractState> Stream<T> outputsOfTypeInternal(Class<T> clazz) {
        return mapNotNullOutputs(it -> clazz.isAssignableFrom(it.getClass()) ? (T) it : null);
    }

    private <T> Stream<T> mapNotNullCommands(Function<Command, T> fun) {
        return transaction.getCommands().stream().map(fun).filter(Objects::nonNull);
    }

    private <T extends CommandData> Stream<Command> commandsOfTypeInternal(Class<T> clazz) {
        return mapNotNullCommands(it -> clazz.isAssignableFrom(it.getValue().getClass()) ? it : null);
    }

    @Override
    public TimeWindow getTime() {
        return transaction.getTime();
    }

    @Override
    public Party getNotary() {
        return transaction.getNotary();
    }

    @Override
    public List<StateAndRef<ContractState>> getInputs() {
        return transaction.getInputs();
    }

    @Override
    public StateAndRef<ContractState> getInput(int index) {
        return transaction.getInputs().get(index);
    }

    @Override
    public List<ContractState> getOutputs() {
        return transaction.getOutputs();
    }

    @Override
    public ContractState getOutput(int index) {
        return transaction.getOutputs().get(index);
    }

    @Override
    public List<Command> getCommands() {
        return transaction.getCommands();
    }

    @Override
    public Command getCommand(int index) {
        return transaction.getCommands().get(index);
    }

    @Override
    public List<SecureHash> getAttachments() {
        return transaction.getAttachments();
    }

    @Override
    public SecureHash getAttachment(int index) {
        return transaction.getAttachments().get(index);
    }

    @Override
    public TransactionStatus getStatus() {
        return transaction.getStatus();
    }

    @Override
    public TransactionView getView() {
        return this;
    }

    @Override
    public <T extends ContractState> List<StateAndRef<T>> inputsOfType(Class<T> clazz) {
        return inputsOfTypeInternal(clazz).collect(Collectors.toList());
    }

    @Override
    public <T extends ContractState> List<StateAndRef<T>> findInputs(Predicate<T> predicate, Class<T> clazz) {
        return inputsOfTypeInternal(clazz).filter(it -> predicate.test(it.getState().getData())).collect(Collectors.toList());
    }

    @Override
    public <T extends ContractState> StateAndRef<T> findInput(Predicate<T> predicate, Class<T> clazz) {
        List<StateAndRef<T>> matches = findInputs(predicate, clazz);
        if (matches.size() != 1) {
            throw new IllegalArgumentException("Expected only one match. Found: " + matches.size());
        }
        return matches.get(0);
    }

    @Override
    public <T extends ContractState> List<T> outputsOfType(Class<T> clazz) {
        return outputsOfTypeInternal(clazz).collect(Collectors.toList());
    }

    @Override
    public <T extends ContractState> List<T> findOutputs(Predicate<T> predicate, Class<T> clazz) {
        return outputsOfTypeInternal(clazz).filter(predicate).collect(Collectors.toList());
    }

    @Override
    public <T extends ContractState> T findOutput(Predicate<T> predicate, Class<T> clazz) {
        List<T> matches = findOutputs(predicate, clazz);
        if (matches.size() != 1) {
            throw new IllegalArgumentException("Expected only one match. Found: " + matches.size());
        }
        return matches.get(0);
    }

    @Override
    public <T extends CommandData> List<Command> commandsOfType(Class<T> clazz) {
        return commandsOfTypeInternal(clazz).collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends CommandData> List<Command> findCommands(Predicate<T> predicate, Class<T> clazz) {
        return commandsOfTypeInternal(clazz).filter(it -> predicate.test((T) it.getValue())).collect(Collectors.toList());
    }

    @Override
    public <T extends CommandData> Command findCommand(Predicate<T> predicate, Class<T> clazz) {
        List<Command> matches = findCommands(predicate, clazz);
        if (matches.size() != 1) {
            throw new IllegalArgumentException("Expected only one match. Found: " + matches.size());
        }
        return matches.get(0);
    }
}
