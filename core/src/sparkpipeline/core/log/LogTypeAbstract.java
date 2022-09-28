package sparkpipeline.core.log;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.LoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public abstract class LogTypeAbstract{
    static final String DEFAULT_LAYOUT_PATTERN = "%d{HH:mm:ss.sss} %p %25.25c %15.15M() : %m%n";
    abstract String nameAppender();
    abstract String typeAppender();
    private final Map<String,String> attribute;
    private final Map<String, Level> componentLogger;
    private String layoutPattern;

    protected LogTypeAbstract(){
        attribute = new HashMap<>();
        layoutPattern = DEFAULT_LAYOUT_PATTERN;
        componentLogger = new HashMap<>();
    }

    public LogTypeAbstract attr(String key, String value){
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        attribute.put(key,value);
        return this;
    }

    public LogTypeAbstract pattern(String pattern){
        Objects.requireNonNull(pattern, "pattern cannot be null");
        this.layoutPattern = pattern;
        return this;
    }

    public LogTypeAbstract logger(String component, Level level){
        Objects.requireNonNull(component, "component cannot be null");
        Objects.requireNonNull(level, "level cannot be null");
        this.componentLogger.put(component,level);
        return this;
    }

    void buildLogger(ConfigurationBuilder<BuiltConfiguration> builder){
        Objects.requireNonNull(builder, "builder cannot be null");

        LayoutComponentBuilder layoutConsole = builder.newLayout("PatternLayout").addAttribute("pattern", layoutPattern);

        AppenderComponentBuilder consoleAppender = builder.newAppender(nameAppender(), typeAppender());
        consoleAppender.add(layoutConsole);

        attribute.keySet().forEach(key -> consoleAppender.addAttribute(key, attribute.get(key)));
        builder.add(consoleAppender);

        componentLogger.forEach((component, level) -> {
            LoggerComponentBuilder logger = builder.newLogger(component, level);
            logger.add(builder.newAppenderRef(nameAppender()));
            logger.addAttribute("additivity", false);
            builder.add(logger);
        });

    }

}
