package sparkpipeline.core.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ControllerExecution {

    private static final int INITIAL_INT_POSITION = 0;

    private int currentPosition = INITIAL_INT_POSITION;
    private Integer size = null;

    private final Integer maxAmountReRunPipeline;
    private final Integer maxAmountReRunEachStep;
    private Map<Integer,Integer> timesAlreadyRunStepMap = new HashMap<>();
    private Integer timesAlreadyRunPipeline = 1;

    public ControllerExecution(Integer maxAmountReRunPipeline, Integer maxAmountReRunEachStep) {
        Objects.requireNonNull(maxAmountReRunPipeline);
        Objects.requireNonNull(maxAmountReRunEachStep);
        this.maxAmountReRunPipeline = maxAmountReRunPipeline;
        this.maxAmountReRunEachStep = maxAmountReRunEachStep;
    }

    private void incrementTimesAlreadyRunStep(){
        if( timesAlreadyRunStepMap.containsKey(currentPosition) ){
            timesAlreadyRunStepMap.put(currentPosition,  timesAlreadyRunStepMap.get(currentPosition) + 1);
        }else{
            timesAlreadyRunStepMap.put(currentPosition, 1);
        }
    }

    void setSize(int size) {
        if( this.size == null ){
            this.size = size;
        }else{
           throw new IllegalArgumentException("size has already been set and cannot be changed");
        }
    }

    public boolean next() {
        runNext();
        return currentPosition <= getSizeRunning();
    }

    public int getTimesAlreadyRunPipeline(){
        return this.timesAlreadyRunPipeline;
    }

    public Integer getTimesAlreadyRunStep(Integer stepPosition){
        return this.timesAlreadyRunStepMap.get(stepPosition);
    }

    public int getSizeRunning() {
        Objects.requireNonNull(size, String.format("size has not been initialized in %s", this.getClass().getName()));
        return size;
    }

    public int getCurrentPosition() {
        return currentPosition;
    }

    private void runNext() {
        currentPosition++;
        incrementTimesAlreadyRunStep();
    }

    public void reRunCurrentStep() {
        if( timesAlreadyRunStepMap.get(currentPosition) <= maxAmountReRunEachStep  ){
            currentPosition--;
        }
    }

    public void reRunAllPipeline() {
        if( timesAlreadyRunPipeline < maxAmountReRunPipeline ){
            timesAlreadyRunPipeline++;
            currentPosition = INITIAL_INT_POSITION;
            timesAlreadyRunStepMap.clear();
        }
    }

    public void abortPipeline() {
        currentPosition = size + 99;
    }

}
