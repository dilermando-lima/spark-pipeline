package sparkpipeline.core.context;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sparkpipeline.core.constant.Msg;

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
    private final ControllerExecution controllerExecution;

    public CommandQueue(DatasetStore datasetStore, ControllerExecution controllerExecution) {
        this.datasetStore = datasetStore;
        this.controllerExecution = controllerExecution;
    }

    public void overrideDatasetStore(DatasetStore datasetStore) {
        Objects.requireNonNull(datasetStore, Msg.DATASETSTORE_CANNOT_BE_NULL);
        this.datasetStore = datasetStore;
    }

    public DatasetStore datasetStore() {
        return datasetStore;
    }

    public ControllerExecution controllerExecution() {
        return controllerExecution;
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

        this.controllerExecution.setSize(commandList.size());

        while (this.controllerExecution.next()) {
            CommandMetadata commandMetadata = commandList.get(this.controllerExecution.getCurrentPosition() -1);
            logger.info("command_index = {} , command_metadata = {}", this.controllerExecution.getCurrentPosition(), commandMetadata);
            commandMetadata.command.changeDatasetStore(context).accept(datasetStore);
        }

    }
}
