package sparkpipeline.core.log;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class LogConfig {

    private final LogConsole logConsole = new LogConsole();
    private LogFile logFile = null;

    public static LogConfig init() {
        return new LogConfig();
    }

    public LogConfig logConsolePattern(String pattern) {
        this.logConsole.pattern(pattern);
        return this;
    }

    public LogConfig logConsoleAttr(String key, String value) {
        this.logConsole.attr(key, value);
        return this;
    }

    public LogConfig logConsoleLevel(String component, Level level) {
        this.logConsole.logger(component, level);
        return this;
    }

    public LogConfig logFilePattern(String pattern) {
        if (this.logFile == null)
            this.logFile = new LogFile();
        this.logFile.pattern(pattern);
        return this;
    }

    public LogConfig logFileAttr(String key, String value) {
        if (this.logFile == null)
            this.logFile = new LogFile();
        this.logFile.attr(key, value);
        return this;
    }

    public LogConfig logFileLevel(String component, Level level) {
        if (this.logFile == null)
            this.logFile = new LogFile();
        this.logFile.logger(component, level);
        return this;
    }

    @SuppressWarnings("java:S4792")
    public void build() {

        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
        logConsole.buildLogger(builder);

        if (logFile != null) logFile.buildLogger(builder);

        Configurator.initialize(builder.build());
        Configurator.setRootLevel(Level.OFF);

    }

}
