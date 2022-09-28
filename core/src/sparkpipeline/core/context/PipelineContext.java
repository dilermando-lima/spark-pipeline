package sparkpipeline.core.context;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sparkpipeline.core.constant.Msg;
import sparkpipeline.core.vars.VarCollectorUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class PipelineContext {

    static final Logger logger = LoggerFactory.getLogger(PipelineContext.class);

    private SparkSession sparkSession = null;
    private final CommandQueue queue;
    private final Map<String, Object> variableMap = new HashMap<>();
    private final Supplier<SparkConf> sparkConfigCreator;

    public PipelineContext(CommandQueue queue, Supplier<SparkConf> sparkConfigCreator) {
        Objects.requireNonNull(queue, Msg.QUEUE_CANNOT_BE_NULL);
        Objects.requireNonNull(sparkConfigCreator, Msg.SPARKCOFIG_CREATOR_CANNOT_BE_NULL);
        Objects.requireNonNull(variableMap, Msg.VARIABLE_MAP_CANNOT_BE_NULL);
        this.queue = queue;
        this.sparkConfigCreator = sparkConfigCreator;
    }

    public DatasetStore datasetStore() {
        return queue.datasetStore();
    }

    public Dataset<Row> datasetByKey(String keyDataset) {
        Objects.requireNonNull(keyDataset, Msg.KEY_DATASET_CANNOT_BE_NULL);
        return queue.datasetStore().datasetByKey(keyDataset);
    }

    public CommandQueue commandQueue() {
        return queue;
    }

    public PipelineContext newVar(String key, Object value) {
        Objects.requireNonNull(key);

        key = VarCollectorUtil.normalizeNameVar(key);
        value = VarCollectorUtil.handleDeclarationsInValues(variableMap, value, key);
        variableMap.put(key, value);
        logger.info("Var '{}={}' has been added into context", key, value);
        return this;
    }

    public String handleStringOnContextVars(String string) {
        return (String) VarCollectorUtil.handleDeclarationsInValues(this.variableMap, string);
    }

    public PipelineContext newVarsFromMap(Map<String, Object> variableMap) {
        Objects.requireNonNull(variableMap);
        this.variableMap.putAll(variableMap);
        VarCollectorUtil.handleDeclarationsInMap(this.variableMap);
        logger.info("Context vars has been changed = {}", this.variableMap);
        return this;
    }

    public Object varByKey(String key) {
        return variableMap.get(VarCollectorUtil.normalizeNameVar(key));
    }

    public <T> T varByKey(String key, Class<T> type) {
        return type.cast(variableMap.get(VarCollectorUtil.normalizeNameVar(key)));
    }

    public SparkSession sparkSession() {
        if (sparkSession == null) {
            logger.info("Starting sparkSession");
            sparkSession = SparkSession.builder()
                            .sparkContext(new SparkContext(sparkConfigCreator.get()))
                            .getOrCreate();
        }
        return sparkSession;
    }

    public void executionReRunCurrentOne() {
        this.commandQueue().doActionIntoExecution(ControllerExecution.ActionType.RE_RUN_CURRENT_ONE);
    }

    public void executionReRunAllPipeline() {
        this.commandQueue().doActionIntoExecution(ControllerExecution.ActionType.START_OVER);
    }

    public void executionAbortAllPipeline() {
        this.commandQueue().doActionIntoExecution(ControllerExecution.ActionType.ABORT_PIPELINE);
    }
}
