package net.corda.core.transactions;

import net.corda.core.contracts.Command;
import net.corda.core.contracts.CommandData;
import net.corda.core.contracts.ContractState;
import net.corda.core.contracts.StateAndRef;
import net.corda.core.crypto.SecureHash;

import java.util.List;
import java.util.function.Predicate;

public interface TransactionView extends NewTransaction {
    StateAndRef<ContractState> getInput(int index);

    List<ContractState> getOutputs();

    ContractState getOutput(int index);

    Command getCommand(int index);

    SecureHash getAttachment(int index);

    <T extends ContractState> List<StateAndRef<T>> inputsOfType(Class<T> clazz);

    <T extends ContractState> List<StateAndRef<T>> findInputs(Predicate<T> predicate, Class<T> clazz);

    <T extends ContractState> StateAndRef<T> findInput(Predicate<T> predicate, Class<T> clazz);

    <T extends ContractState> List<T> outputsOfType(Class<T> clazz);

    <T extends ContractState> List<T> findOutputs(Predicate<T> predicate, Class<T> clazz);

    <T extends ContractState> T findOutput(Predicate<T> predicate, Class<T> clazz);

    <T extends CommandData> List<Command> commandsOfType(Class<T> clazz);

    <T extends CommandData> List<Command> findCommands(Predicate<T> predicate, Class<T> clazz);

    <T extends CommandData> Command findCommand(Predicate<T> predicate, Class<T> clazz);
}
