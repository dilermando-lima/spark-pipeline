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

public class WriterCSV extends AbstractWriter<WriterCSV> {

    static final Logger logger = LoggerFactory.getLogger(WriterCSV.class);

    private final String path;
    private final Map<String, String> options;
    private SaveMode saveMode = SaveMode.ErrorIfExists;

    public WriterCSV(Map<String, String> options, String path) {
        Objects.requireNonNull(path, "path cannot be null");
        this.path = path;
        this.options = options == null ? new HashMap<>() : options;
    }

    public static WriterCSV init(String path) {
        return new WriterCSV(null, path);
    }

    public static WriterCSV initWithOptions(Map<String, String> options, String path) {
        return new WriterCSV(options, path);
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

    private String handlePathFromContext(PipelineContext context) {
        logger.info("paths to be handled {}", path);
        String pathHandled = context.handleStringOnContextVars(path);
        logger.info("paths handled on context {}", pathHandled);
        return pathHandled;
    }

    @Override
    public BiConsumer<PipelineContext, Dataset<Row>> writeImplementation() {
        return (context, dataset) -> dataset.write()
                .mode(saveMode)
                .options(handleOptionsFromContext(context))
                .csv(handlePathFromContext(context));

    }

    @Override
    public WriterCSV returnChildImplementation() {
        return this;
    }

    public WriterCSV delimiter(String value) {
        Objects.requireNonNull(value, "delimiter cannot be null");
        this.options.put("sep", value);
        return this;
    }

    public WriterCSV quote(String value) {
        Objects.requireNonNull(value, "quote cannot be null");
        this.options.put("quote", value);
        return this;
    }

    public WriterCSV withHeader(boolean value) {
        this.options.put("header", value ? "true" : "false");
        return this;
    }

    public WriterCSV dateFormat(String value) {
        Objects.requireNonNull(value, "dateFormat cannot be null");
        this.options.put("dateFormat", value);
        return this;
    }

    public WriterCSV encode(String value) {
        Objects.requireNonNull(value, "encode cannot be null");
        this.options.put("encoding", value);
        return this;
    }

    public WriterCSV saveMode(SaveMode saveMode) {
        Objects.requireNonNull(saveMode, "modeSave cannot be null");
        this.saveMode = saveMode;
        return this;
    }

    public WriterCSV option(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        this.options.put(key, value);
        return this;
    }
}
