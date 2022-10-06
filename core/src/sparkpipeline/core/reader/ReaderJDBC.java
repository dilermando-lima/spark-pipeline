package sparkpipeline.core.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkpipeline.core.context.PipelineContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ReaderJDBC extends AbstractReader<ReaderJDBC> {

    static final Logger logger = LoggerFactory.getLogger(ReaderJDBC.class);

    private final Map<String, String> options;
    private StructType schema = null;

    public ReaderJDBC(Map<String, String> options) {
        this.options = options == null ? new HashMap<>() : options;
    }

    public static ReaderJDBC init() {
        return new ReaderJDBC(null);
    }

    public static ReaderJDBC initWithOptions(Map<String, String> options) {
        return new ReaderJDBC(options);
    }

    private Map<String, String> handleOptionsFromContext(PipelineContext context) {
        if (options.isEmpty())
            return options;

        logger.info("options to be handled {}", options);
        Map<String, String> optionsHandled = options.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, entry -> context.handleStringFromContextVars(entry.getValue())));
        logger.info("options handled on context {}", optionsHandled);
        return optionsHandled;
    }

    @Override
    public Function<PipelineContext, Dataset<Row>> readImplementation() {
        return context -> context.sparkSession()
                .read()
                .format("jdbc")
                .schema(schema)
                .options(handleOptionsFromContext(context))
                .load();
    }

    @Override
    public ReaderJDBC returnChildImplementation() {
        return this;
    }

    public ReaderJDBC url(String value) {
        Objects.requireNonNull(value, "url cannot be null");
        this.options.put("url", value);
        return this;
    }

    public ReaderJDBC user(String value) {
        Objects.requireNonNull(value, "user cannot be null");
        this.options.put("user", value);
        return this;
    }

    public ReaderJDBC password(String value) {
        Objects.requireNonNull(value, "password cannot be null");
        this.options.put("password", value);
        return this;
    }

    public ReaderJDBC driver(String value) {
        Objects.requireNonNull(value, "driver cannot be null");
        this.options.put("driver", value);
        return this;
    }

    public ReaderJDBC schema(StructType schema) {
        this.schema = schema;
        return this;
    }

    public ReaderJDBC option(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        this.options.put(key, value);
        return this;
    }

}
