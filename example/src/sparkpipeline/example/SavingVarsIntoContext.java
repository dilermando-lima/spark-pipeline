package sparkpipeline.example;

import java.time.LocalDateTime;
import java.util.Arrays;

import sparkpipeline.core.pipeline.Pipeline;

class SavingVarsIntoContext {

    @SuppressWarnings({"java:S1192","java:S106"})
    public static void main(String[] args) {

        Pipeline.init()
                // saving var into context at first pipeline step
                .anyRunning(context -> {
                    context.newVar("VAR_1", "TESTE");
                    context.newVar("VAR_2", 4);
                    context.newVar("VAR_3", LocalDateTime.now());
                    context.newVar("VAR_4", new String[]{"value1-from-var4", "value2-from-var4"});
                })
                // retrieving vars from context
                .anyRunning(context -> {

                    Object var1AsObject = context.varByKey("VAR_1");
                    String var1AsString = context.varByKey("VAR_1",String.class);
                    String var1FromCast =  (String) context.varByKey("VAR_1");

                    Integer var2 = context.varByKey("VAR_2",Integer.class);
                    LocalDateTime var3 = context.varByKey("VAR_3",LocalDateTime.class);
                    String[] var4 = context.varByKey("VAR_4",String[].class);

                    Object varNotFound = context.varByKey("any-var-not-found");

                    String varUsingVarDeclarations = context.handleStringFromContextVars("${VAR_1} , ${VAR_2} AND ${VAR_NOT_FOUND:DEFAULT_VALUE}");

                    System.out.println("==== PRINT VARS ======");
                    System.out.println("var1AsObject: " + var1AsObject);
                    System.out.println("var1AsString: " + var1AsString);
                    System.out.println("var1FromCast: " + var1FromCast);
                    System.out.println("var2: "         + var2);
                    System.out.println("var3: "         + var3);
                    System.out.println("var4: "         + Arrays.toString(var4));
                    System.out.println("varNotFound: "  + varNotFound);
                    System.out.println("varUsingVarDeclarations: "  + varUsingVarDeclarations);
                    System.out.println("====                      ======");
          

                })
                .execute();
    }
}