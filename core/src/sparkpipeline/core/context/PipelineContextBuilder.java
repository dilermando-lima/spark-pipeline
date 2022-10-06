package sparkpipeline.core.context;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sparkpipeline.core.log.LogConfig;
import sparkpipeline.core.vars.VarCollector;
import sparkpipeline.core.vars.VarCollectorApplication;
import sparkpipeline.core.vars.VarCollectorFile;
import sparkpipeline.core.vars.VarCollectorMap;

public class PipelineContextBuilder {

    Logger logger;

    private final List<VarCollector> collectorList = new LinkedList<>();
    private LogConfig logConfig = null;
    private PipelineContext context = null;
    private static final Function<PipelineContext,SparkConf>  DEFAULT_SPARK_CONFIG_BUILDER = c -> new SparkConf()
            .setAppName("DEFAULT-APP")
            .setMaster("local[1]")
            .set("spark.sql.shuffle.partitions", "1")
            .set("spark.ui.enabled", "false");

    private Function<PipelineContext,SparkConf> sparkConfigBuilder = null;

    private final List<Runnable> afterStartLogConfig = new LinkedList<>();
    private final List<Runnable> beforeStartContext = new LinkedList<>();
    private final List<Consumer<PipelineContext>> afterStartContext = new LinkedList<>();
    private final List<Consumer<PipelineContext>> afterRetrieveAllContextVars = new LinkedList<>();

    public static PipelineContextBuilder init() {
        return new PipelineContextBuilder();
    }

    public PipelineContextBuilder addVarCollector(VarCollector collector) {
        if (collector != null)
            collectorList.add(collector);
        return this;
    }

    public PipelineContextBuilder collectVarsFromArgs(String[] appArgs) {
        collectorList.add(VarCollectorApplication.init(appArgs));
        return this;
    }

    public PipelineContextBuilder collectVarsFromFile(String path) {
        collectorList.add(VarCollectorFile.init(path));
        return this;
    }

    public PipelineContextBuilder collectVarsFromMap(Map<String, Object> varsMap) {
        collectorList.add(VarCollectorMap.init(varsMap));
        return this;
    }

    public PipelineContextBuilder setLogConfig(LogConfig logConfig) {
        this.logConfig = logConfig;
        return this;
    }

    public PipelineContextBuilder afterStartLogConfig(Runnable runnable) {
        if (runnable != null)
            afterStartLogConfig.add(runnable);
        return this;
    }

    public PipelineContextBuilder beforeStartContext(Runnable runnable) {
        if (runnable != null)
            beforeStartContext.add(runnable);
        return this;
    }

    public PipelineContextBuilder afterStartContext(Consumer<PipelineContext> consumer) {
        if (consumer != null)
            afterStartContext.add(consumer);
        return this;
    }

    public PipelineContextBuilder afterRetrieveAllContextVars(Consumer<PipelineContext> consumer) {
        if (consumer != null)
            afterRetrieveAllContextVars.add(consumer);
        return this;
    }

    public PipelineContextBuilder sparkConfigBuilder(Function<PipelineContext,SparkConf> sparkConfigBuilder) {
        this.sparkConfigBuilder = sparkConfigBuilder;
        return this;
    }

    public PipelineContext startPipelineContext() {

        beforeStartContext.forEach(Runnable::run);

        if (context != null) {
            logger.info("Reusing PipelineContext has already been started");
            afterStartLogConfig.forEach(Runnable::run);
            afterStartContext.forEach(consumer -> consumer.accept(context));
            afterRetrieveAllContextVars.forEach(consumer -> consumer.accept(context));
            return context;
        }

        if (logConfig != null)
            logConfig.build();

        logger = LoggerFactory.getLogger(PipelineContextBuilder.class);
        logger.info("Starting PipelineContext");

        afterStartLogConfig.forEach(Runnable::run);

        context = new PipelineContext(
                new CommandQueue(new DatasetStore()),
                sparkConfigBuilder == null ? DEFAULT_SPARK_CONFIG_BUILDER : sparkConfigBuilder);

        afterStartContext.forEach(consumer -> consumer.accept(context));

        for (VarCollector varCollector : collectorList) {
            context.newVarsFromMap(varCollector.collectVarMap(context));
        }

        afterRetrieveAllContextVars.forEach(consumer -> consumer.accept(context));
        return context;
    }

}
