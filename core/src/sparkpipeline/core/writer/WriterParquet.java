package sparkpipeline.core.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparkpipeline.core.context.PipelineContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class WriterParquet extends AbstractWriter<WriterParquet> {

    static final Logger logger = LoggerFactory.getLogger(WriterParquet.class);

    private final String path;
    private final Map<String, String> options;
    private SaveMode saveMode = SaveMode.ErrorIfExists;

    public WriterParquet(Map<String, String> options, String path) {
        Objects.requireNonNull(path, "path cannot be null");
        this.path = path;
        this.options = options == null ? new HashMap<>() : options;
    }

    public static WriterParquet init(String path) {
        return new WriterParquet(null, path);
    }

    public static WriterParquet initWithOptions(Map<String, String> options, String path) {
        return new WriterParquet(options, path);
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

    private String handlePathFromContext(PipelineContext context) {
        logger.info("paths to be handled {}", path);
        String pathHandled = context.handleStringFromContextVars(path);
        logger.info("paths handled on context {}", pathHandled);
        return pathHandled;
    }

    @Override
    public BiConsumer<PipelineContext, Dataset<Row>> writeImplementation() {
        return (context, dataset) -> dataset.write()
                .mode(saveMode)
                .options(handleOptionsFromContext(context))
                .parquet(handlePathFromContext(context));

    }

    @Override
    public WriterParquet returnChildImplementation() {
        return this;
    }

    public WriterParquet saveMode(SaveMode saveMode) {
        Objects.requireNonNull(saveMode, "modeSave cannot be null");
        this.saveMode = saveMode;
        return this;
    }

    public WriterParquet option(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        this.options.put(key, value);
        return this;
    }
}
