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

public class ReaderParquet extends AbstractReader<ReaderParquet> {

    static final Logger logger = LoggerFactory.getLogger(ReaderParquet.class);

    private final String[] path;
    private final Map<String, String> options;
    private StructType schema = null;

    public ReaderParquet(Map<String, String> options, String... path) {
        Objects.requireNonNull(path, "path cannot be null");
        if (Arrays.stream(path).anyMatch(Objects::isNull))
            throw new NullPointerException("path cannot has any null elements");
        this.path = path;
        this.options = options == null ? new HashMap<>() : options;
    }

    public static ReaderParquet init(String... path) {
        return new ReaderParquet(null, path);
    }

    public static ReaderParquet initWithOptions(Map<String, String> options, String... path) {
        return new ReaderParquet(options, path);
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

    private String[] handlePathsFromContext(PipelineContext context) {
        logger.info("paths to be handled {}", Arrays.asList(path));
        String[] pathsHandled = Arrays.stream(path).map(context::handleStringFromContextVars).toArray(String[]::new);
        logger.info("paths handled on context {}", Arrays.asList(pathsHandled));
        return pathsHandled;
    }

    @Override
    public Function<PipelineContext, Dataset<Row>> readImplementation() {
        return context -> context.sparkSession()
                .read()
                .schema(schema)
                .options(handleOptionsFromContext(context))
                .parquet(handlePathsFromContext(context));
    }

    @Override
    public ReaderParquet returnChildImplementation() {
        return this;
    }

    public ReaderParquet option(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        this.options.put(key, value);
        return this;
    }

    public ReaderParquet schema(StructType schema) {
        this.schema = schema;
        return this;
    }

}
