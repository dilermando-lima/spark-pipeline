package sparkpipeline.core.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkpipeline.core.context.PipelineContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ReaderJSON extends AbstractReader<ReaderJSON> {

    static final Logger logger = LoggerFactory.getLogger(ReaderJSON.class);

    private final String[] path;
    private final Map<String, String> options;
    private StructType schema = null;

    public ReaderJSON(Map<String, String> options, String... path) {
        Objects.requireNonNull(path, "path cannot be null");
        if (Arrays.stream(path).anyMatch(Objects::isNull))
            throw new NullPointerException("path cannot has any null elements");
        this.path = path;
        this.options = options == null ? new HashMap<>() : options;
    }

    public static ReaderJSON init(String... path) {
        return new ReaderJSON(null, path);
    }

    public static ReaderJSON initWithOptions(Map<String, String> options, String... path) {
        return new ReaderJSON(options, path);
    }

    private Map<String, String> handleOptionsFromContext(PipelineContext context) {
        if (options.isEmpty())
            return options;

        logger.info("options to be handled {}", options);
        Map<String, String> optionsHandled = options.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, entry -> context.handleStringOnContextVars(entry.getValue())));
        logger.info("options handled on context {}", optionsHandled);
        return optionsHandled;
    }

    private String[] handlePathsFromContext(PipelineContext context) {
        logger.info("paths to be handled {}", Arrays.asList(path));
        String[] pathsHandled = Arrays.stream(path).map(context::handleStringOnContextVars).toArray(String[]::new);
        logger.info("paths handled on context {}", Arrays.asList(pathsHandled));
        return pathsHandled;
    }

    @Override
    public Function<PipelineContext, Dataset<Row>> readImplementation() {
        return context -> context.sparkSession()
                .read()
                .schema(schema)
                .options(handleOptionsFromContext(context))
                .json(handlePathsFromContext(context));
    }

    @Override
    public ReaderJSON returnChildImplementation() {
        return this;
    }

    public ReaderJSON dateFormat(String value) {
        Objects.requireNonNull(value, "dateFormat cannot be null");
        this.options.put("dateFormat", value);
        return this;
    }

    public ReaderJSON schema(StructType schema) {
        this.schema = schema;
        return this;
    }

    public ReaderJSON encode(String value) {
        Objects.requireNonNull(value, "encode cannot be null");
        this.options.put("encoding", value);
        return this;
    }

    public ReaderJSON option(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        this.options.put(key, value);
        return this;
    }

}
