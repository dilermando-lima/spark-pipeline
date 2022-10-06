package sparkpipeline.core.context;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sparkpipeline.core.constant.Msg;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class CommandQueue {

    public enum TypeCommand {
        READ,
        WRITE,
        TRANSFORM,
        PERSIST,
        UN_PERSIST,
        REMOVE,
        ANY_RUNNING,
        VALIDATE,
        ;
    }

    private static class CommandMetadata {
        final DatasetStoreCommand command;
        final TypeCommand type;
        final String description;

        public CommandMetadata(DatasetStoreCommand command, TypeCommand type, String description) {
            this.command = command;
            this.type = type;
            this.description = description;
        }

        @Override
        public String toString() {
            return (type != null ? " type = " + type.name() : "") +
                    (description != null ? ", description = " + description : "");
        }
    }

    static final Logger logger = LoggerFactory.getLogger(CommandQueue.class);

    private final List<CommandMetadata> commandList = new LinkedList<>();
    private DatasetStore datasetStore;
    private final ControllerExecution controllerExecution = new ControllerExecution();

    public CommandQueue(DatasetStore datasetStore) {
        this.datasetStore = datasetStore;
    }

    public void overrideDatasetStore(DatasetStore datasetStore) {
        Objects.requireNonNull(datasetStore, Msg.DATASETSTORE_CANNOT_BE_NULL);
        this.datasetStore = datasetStore;
    }

    public void doActionIntoExecution(ControllerExecution.ActionType action,Integer times) {
        this.controllerExecution.doAction(action,times);
    }

    public DatasetStore datasetStore() {
        return datasetStore;
    }

    public Dataset<Row> datasetByKey(String keyDataset) {
        logger.info("keyDataset = {}", keyDataset);
        Objects.requireNonNull(datasetStore, Msg.KEY_DATASET_CANNOT_BE_NULL);
        return datasetStore.datasetByKey(keyDataset);
    }

    public void addCommand(DatasetStoreCommand command, TypeCommand type, String description) {
        logger.info("type = {}, description = {}", type, description);
        Objects.requireNonNull(command, Msg.COMMAND_CANNOT_BE_NULL);
        commandList.add(new CommandMetadata(command, type, description));
    }

    public void addCommand(DatasetStoreCommand command, TypeCommand type) {
        addCommand(command, type, null);
    }

    public void executeQueue(PipelineContext context) {
        logger.info("command_qty = {}", commandList.size());
        Objects.requireNonNull(context, Msg.CONTEXT_CANNOT_BE_NULL);
        Objects.requireNonNull(datasetStore, Msg.DATASETSTORE_CANNOT_BE_NULL);

        this.controllerExecution.setSizeRunning(commandList.size() - 1);

        while (this.controllerExecution.hasNext()) {
            CommandMetadata commandMetadata = commandList.get(this.controllerExecution.getCurrentRunning());
            logger.info("command_index = {} , command_metadata = {}", this.controllerExecution.getCurrentRunning(),
                    commandMetadata);
            commandMetadata.command.changeDatasetStore(context).accept(datasetStore);
            this.controllerExecution.doAction(ControllerExecution.ActionType.RUN_NEXT,null);
        }

    }
}
