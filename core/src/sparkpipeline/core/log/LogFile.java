package sparkpipeline.core.log;

public class LogFile extends  LogTypeAbstract{
    @Override
    String nameAppender() {
        return "file";
    }

    @Override
    String typeAppender() {
        return "File";
    }
}
