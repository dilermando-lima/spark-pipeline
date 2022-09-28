package sparkpipeline.core.vars;

import sparkpipeline.core.context.PipelineContext;

import java.util.Map;
import java.util.stream.Stream;

public class VarCollectorApplication implements VarCollector {

    final String[] appArgs;

    private VarCollectorApplication(String[] appArgs) {
        this.appArgs = appArgs;
    }

    public static VarCollectorApplication init(String[] appArgs) {
        return new VarCollectorApplication(appArgs == null ? new String[] {} : appArgs);
    }

    @Override
    public Map<String, Object> collectVarMap(PipelineContext pipelineContext) {
        return Stream.of(appArgs)
                .collect(VarCollectorUtil.collectVarsToMap("=", "--"));
    }
}
