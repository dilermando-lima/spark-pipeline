package sparkpipeline.core.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkpipeline.core.context.CommandQueue;
import sparkpipeline.core.context.PipelineContext;
import sparkpipeline.core.context.PipelineContextBuilder;
import sparkpipeline.core.reader.AbstractReader;
import sparkpipeline.core.writer.AbstractWriter;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class Pipeline {

    Logger logger;
    private final PipelineContext pipelineContext;

    public Pipeline(PipelineContextBuilder contextBuilder) {
        contextBuilder.afterStartLogConfig(() -> logger = LoggerFactory.getLogger(Pipeline.class));
        this.pipelineContext = contextBuilder.startPipelineContext();
    }

    public static Pipeline initWithContextBuilder(PipelineContextBuilder contextBuilder) {
        return new Pipeline(contextBuilder);
    }

    public static Pipeline init() {
        return new Pipeline(new PipelineContextBuilder());
    }

    public PipelineContext context() {
        return pipelineContext;
    }

    public Pipeline read(String keyDataset, AbstractReader<?> reader) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> {
                    reader.overrideContext(context);
                    datasetStore.save(keyDataset, reader.buildDataset());
                },
                CommandQueue.TypeCommand.READ,
                String.format("Reading into %s", keyDataset));
        return this;
    }

    public Pipeline transform(String newKeyDataset, Function<PipelineContext, Dataset<Row>> transformation) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> datasetStore.save(newKeyDataset, transformation.apply(context)),
                CommandQueue.TypeCommand.TRANSFORM,
                String.format("Adding Transformation into %s", newKeyDataset));
        return this;
    }

    public Pipeline transform(String newKeyDataset, Function<PipelineContext, Dataset<Row>> transformation,
            String comments) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> datasetStore.save(newKeyDataset, transformation.apply(context)),
                CommandQueue.TypeCommand.TRANSFORM,
                comments);
        return this;
    }

    public Pipeline transformSql(String keyDatasetToTransform, Function<PipelineContext, String> transformation) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> datasetStore.save(
                        keyDatasetToTransform,
                        context.sparkSession()
                                .sqlContext()
                                .sql(transformation.apply(context))),
                CommandQueue.TypeCommand.TRANSFORM,
                String.format("Adding Transformation into %s", keyDatasetToTransform));
        return this;
    }

    public Pipeline transformSql(String keyDatasetToTransform, Function<PipelineContext, String> transformation,
            String comments) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> datasetStore.save(
                        keyDatasetToTransform,
                        context.sparkSession()
                                .sqlContext()
                                .sql(transformation.apply(context))),
                CommandQueue.TypeCommand.TRANSFORM,
                comments);
        return this;
    }

    public Pipeline write(String keyDataset, AbstractWriter<?> writer) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> {
                    writer.overrideContext(context);
                    writer.write(datasetStore.datasetByKey(keyDataset));
                },
                CommandQueue.TypeCommand.WRITE,
                String.format("Writing from %s", keyDataset));
        return this;
    }

    public Pipeline persist(String... keyDataset) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> datasetStore.persist(keyDataset),
                CommandQueue.TypeCommand.PERSIST,
                String.format("Persisting %s", Arrays.toString(keyDataset)));
        return this;
    }

    public Pipeline unpersist(String... keyDataset) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> datasetStore.unpersist(keyDataset),
                CommandQueue.TypeCommand.UN_PERSIST,
                String.format("UnPersisting %s", Arrays.toString(keyDataset)));
        return this;
    }

    public Pipeline remove(String... keyDataset) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> datasetStore.remove(keyDataset),
                CommandQueue.TypeCommand.REMOVE,
                String.format("Removing %s", Arrays.toString(keyDataset)));
        return this;
    }

    public Pipeline validate(Consumer<PipelineContext> validation) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> validation.accept(context),
                CommandQueue.TypeCommand.VALIDATE);
        return this;
    }

    public Pipeline validate(Consumer<PipelineContext> validation, String comments) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> validation.accept(context),
                CommandQueue.TypeCommand.VALIDATE, comments);
        return this;
    }

    public Pipeline anyRunning(Consumer<PipelineContext> running) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> running.accept(context),
                CommandQueue.TypeCommand.ANY_RUNNING);
        return this;
    }

    public Pipeline anyRunning(Consumer<PipelineContext> running, String comments) {
        pipelineContext.commandQueue().addCommand(
                context -> datasetStore -> running.accept(context),
                CommandQueue.TypeCommand.ANY_RUNNING, comments);
        return this;
    }

    public void execute() {
        execute(c -> true);
    }

    public void execute(Function<PipelineContext, Boolean> executeCondition) {
        Objects.requireNonNull(executeCondition, "executeCondition cannot be null");
        if (Boolean.TRUE.equals(executeCondition.apply(pipelineContext))) {
            Objects.requireNonNull(pipelineContext, "context cannot be null");
            Objects.requireNonNull(pipelineContext.commandQueue(), "context.commandQueue cannot be null");
            logger.info("executing all commands");
            pipelineContext.commandQueue().executeQueue(pipelineContext);
        } else {
            logger.info("execution has not been applied on condition then was skipped");
        }
    }
}
