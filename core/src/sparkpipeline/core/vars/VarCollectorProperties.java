package sparkpipeline.core.vars;

import sparkpipeline.core.context.PipelineContext;

import java.util.Map;
import java.util.Objects;

public class VarCollectorProperties implements VarCollector {

    private final String path;

    private VarCollectorProperties(String path) {
        Objects.requireNonNull(path, "path cannot be null");
        this.path = path;
    }

    public static VarCollectorProperties init(String path) {
        return new VarCollectorProperties(path);
    }

    @Override
    public Map<String, Object> collectVarMap(PipelineContext pipelineContext) {
        return pipelineContext.sparkSession()
                .read().textFile(pipelineContext.handleStringOnContextVars(path))
                .collectAsList().stream()
                .collect(VarCollectorUtil.collectVarsToMap("=", ""));
    }
}
