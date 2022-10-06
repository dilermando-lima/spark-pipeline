package sparkpipeline.example;

import java.time.Duration;
import java.util.Objects;

public class Test {

    public class ControllerExecution {

        private class ControllerItem {
          private int times;
          private Duration delay;
          private String key;
          private int positionRunning;
        }

        private static final int INITIAL_INT_RUNNING = 0;

        private int currentRunning = INITIAL_INT_RUNNING;
        private Integer sizeRunning = null;
   

        public boolean next(){
            Objects.requireNonNull(sizeRunning,"sizeRunning has not been set in %s");
            return currentRunning <= sizeRunning;
        }

        public void setSizeRunning(int size) {
            this.sizeRunning = size;
        }

        public int getCurrentRunning() {
            return currentRunning;
        }

        public void reRunCurrentStep(String key, int times, Duration delay){
            currentRunning--;
        }

        public void reRunAllPipeline(String key, int times, Duration delay){
            currentRunning = INITIAL_INT_RUNNING - 1;
        }

        public void abortPipeline(){
            currentRunning = sizeRunning + 1;
        }







    }


    public static void main(String[] args) {
        
    }
    
}
