package sparkpipeline.core.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import sparkpipeline.core.constant.Msg;

public class ControllerExecution {

    public enum ActionType {

        RUN_NEXT(controllerExecution -> controllerExecution.currentRunning++),
        RE_RUN_CURRENT_ONE(controllerExecution -> controllerExecution.currentRunning--),
        ABORT_PIPELINE(controllerExecution -> controllerExecution.currentRunning = controllerExecution.sizeRunning + 1),
        START_OVER(controllerExecution -> controllerExecution.currentRunning = INITIAL_INT_RUNNING - 1),
        ;

        private final Consumer<ControllerExecution> action;

        private ActionType(Consumer<ControllerExecution> action) {
            this.action = action;
        }

        public Consumer<ControllerExecution> action() {
            return action;
        }
    }

    private static final Map<String, Integer> timesExecutionControllerMap = new HashMap<>();

    private void setTimesExecutionToCurrentRunning(Integer times, ActionType action) {

        String keyTimesExecutionController = action.name() + "-" + getCurrentRunning();

        if (timesExecutionControllerMap.containsKey(keyTimesExecutionController)){
            throw new IllegalArgumentException(
                    String.format(
                            "Action '%s' has already been set in step '%s' as '%s' times",
                            action.name(),
                            getCurrentRunning(),
                            timesExecutionControllerMap.get(keyTimesExecutionController)));
        }

        times = (times == null || times < 0) ? 0 : times;

        timesExecutionControllerMap.put(keyTimesExecutionController, times);

    }

    private static final int INITIAL_INT_RUNNING = 0;

    private int currentRunning = INITIAL_INT_RUNNING;
    private Integer sizeRunning = null;

    public void setSizeRunning(int size) {
        this.sizeRunning = size;
    }

    public boolean hasNext() {
        return currentRunning <= getSizeRunning();
    }

    public int getSizeRunning() {
        Objects.requireNonNull(sizeRunning,
                String.format("sizeRunning has not been initialized in %s", this.getClass().getName()));
        return sizeRunning;
    }

    public int getCurrentRunning() {
        return currentRunning;
    }

    public void doAction(ActionType action, Integer times) {
        Objects.requireNonNull(action, Msg.ACTION_CANNOT_BE_NULL);
        action.action().accept(this);
    }

}
