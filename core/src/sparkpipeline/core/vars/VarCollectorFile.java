package sparkpipeline.core.vars;

import sparkpipeline.core.context.PipelineContext;

import java.util.Map;
import java.util.Objects;

public class VarCollectorFile implements VarCollector {

    private final String path;
    public static final String ENCODING_DEFAULT = "UTF-8";
    private String encoding;

    private VarCollectorFile(String path, String encoding) {
        Objects.requireNonNull(path, "path cannot be null");
        this.path = path;
        this.encoding = encoding;
    }

    public static VarCollectorFile init(String path) {
        return new VarCollectorFile(path,null);
    }

    public static VarCollectorFile initWithEncoding(String path, String encoding) {
        return new VarCollectorFile(path, encoding);
    }

    @Override
    public Map<String, Object> collectVarMap(PipelineContext context) {
        return context.sparkSession()
                .read()
                .option("encoding", encoding == null ? ENCODING_DEFAULT : context.handleStringFromContextVars(encoding))
                .textFile(context.handleStringFromContextVars(path))
                .collectAsList().stream()
                .collect(VarCollectorUtil.collectVarsToMap("=", ""));
    }

}
