package sparkpipeline.core.vars;

import sparkpipeline.core.context.PipelineContext;

import java.util.Map;
import java.util.Objects;

public class VarCollectorMap implements VarCollector {

    private final Map<String, Object> varsMap;

    private VarCollectorMap(Map<String, Object> varsMap) {
        Objects.requireNonNull(varsMap, "varsMap cannot be null");
        this.varsMap = varsMap;
    }

    public static VarCollectorMap init(Map<String, Object> varsMap) {
        return new VarCollectorMap(varsMap);
    }

    @Override
    public Map<String, Object> collectVarMap(PipelineContext pipelineContext) {
        return varsMap;
    }
}
