package sparkpipeline.core.context;

import java.util.Objects;
import java.util.function.Consumer;

import sparkpipeline.core.constant.Msg;

public class ControllerExecution {

    public enum ActionType {
        
        RUN_NEXT(controllerExecution -> controllerExecution.currentRunning++),
        RE_RUN_CURRENT_ONE(controllerExecution -> controllerExecution.currentRunning = controllerExecution.currentRunning - 2),
        ABORT_PIPELINE(controllerExecution -> controllerExecution.currentRunning = controllerExecution.sizeRunning + 1),
        START_OVER(controllerExecution -> controllerExecution.currentRunning = INITIAL_INT_RUNNING),
        ;

        private final Consumer<ControllerExecution> action;

        private ActionType(Consumer<ControllerExecution> action) {
            this.action = action;
        }

        public Consumer<ControllerExecution> action() {
            return action;
        }
    }

    public static final int INITIAL_INT_RUNNING = 0;

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

    public void doAction(ActionType action) {
        Objects.requireNonNull(action, Msg.ACTION_CANNOT_BE_NULL);
        action.action().accept(this);
    }

}
