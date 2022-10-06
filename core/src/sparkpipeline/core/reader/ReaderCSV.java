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

public class ReaderCSV extends AbstractReader<ReaderCSV> {

    static final Logger logger = LoggerFactory.getLogger(ReaderCSV.class);

    private final String[] path;
    private final Map<String, String> options;
    private StructType schema = null;

    public ReaderCSV(Map<String, String> options, String... path) {
        Objects.requireNonNull(path, "path cannot be null");
        if (Arrays.stream(path).anyMatch(Objects::isNull))
            throw new NullPointerException("path cannot has any null elements");
        this.path = path;
        this.options = options == null ? new HashMap<>() : options;
    }

    public static ReaderCSV init(String... path) {
        return new ReaderCSV(null, path);
    }

    public static ReaderCSV initWithOptions(Map<String, String> options, String... path) {
        return new ReaderCSV(options, path);
    }

    private Map<String, String> handleOptionsFromContext(PipelineContext context) {
        if (options.isEmpty())
            return options;

        logger.info("options to be handled {}", options);
        Map<String, String> optionsHandled = options.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> context.handleStringFromContextVars(entry.getValue())));
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
                .csv(handlePathsFromContext(context));
    }

    @Override
    public ReaderCSV returnChildImplementation() {
        return this;
    }

    public ReaderCSV delimiter(String value) {
        Objects.requireNonNull(value, "delimiter cannot be null");
        this.options.put("sep", value);
        return this;
    }

    public ReaderCSV encode(String value) {
        Objects.requireNonNull(value, "encode cannot be null");
        this.options.put("encoding", value);
        return this;
    }

    public ReaderCSV schema(StructType schema) {
        this.schema = schema;
        return this;
    }

    public ReaderCSV quote(String value) {
        Objects.requireNonNull(value, "quote cannot be null");
        this.options.put("quote", value);
        return this;
    }

    public ReaderCSV hasHeader(boolean value) {
        this.options.put("header", value ? "true" : "false");
        return this;
    }

    public ReaderCSV dateFormat(String value) {
        Objects.requireNonNull(value, "dateFormat cannot be null");
        this.options.put("dateFormat", value);
        return this;
    }

    public ReaderCSV option(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        this.options.put(key, value);
        return this;
    }

}
