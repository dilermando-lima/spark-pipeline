package sparkpipeline.core.log;

public class LogConsole extends  LogTypeAbstract{
    @Override
    String nameAppender() {
        return "console";
    }

    @Override
    String typeAppender() {
        return "Console";
    }
}
