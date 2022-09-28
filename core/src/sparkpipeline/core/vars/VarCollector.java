package sparkpipeline.core.vars;

import sparkpipeline.core.context.PipelineContext;

import java.util.Map;

@FunctionalInterface
public interface VarCollector {
    public Map<String, Object> collectVarMap(PipelineContext pipelineContext);
}
