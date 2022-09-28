package sparkpipeline.core.vars;

import java.text.Normalizer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class VarCollectorUtil {

    private VarCollectorUtil() {
    }

    public static String normalizeNameVar(String nameVar) {
        if (nameVar == null)
            return "";

        return Normalizer.normalize(nameVar, Normalizer.Form.NFKD)
                .replaceAll("\\p{M}", "")
                .trim()
                .replace(" ", "_")
                .replace(".", "_")
                .replace("-", "_")
                .toUpperCase();
    }

    public static Object handleDeclarationsInValues(Map<String, Object> mapVars, Object value) {
        return handleDeclarationsInValues(mapVars, value, null);
    }

    @SuppressWarnings("java:S3776")
    public static Object handleDeclarationsInValues(Map<String, Object> mapVars, Object valueObject,
            String nameVarToAvoidRecursion) {
        if (valueObject == null)
            return null;
        if (!(valueObject instanceof String))
            return valueObject;

        String value = (String) valueObject;

        nameVarToAvoidRecursion = normalizeNameVar(nameVarToAvoidRecursion);

        Pattern r = Pattern.compile("\\$\\{.{0,255}}");
        Matcher m = r.matcher(value);
        while (m.find()) {
            String keyReplacementGroup = m.group();
            String keyReplacementNameVar = normalizeNameVar(m.group().replaceAll("^\\$\\{", "").replaceAll("}$", ""));

            if (keyReplacementNameVar.trim().equals(""))
                throw new IllegalArgumentException("name var between ${} cannot be empty");

            String defaultValue = "";

            if (keyReplacementNameVar.contains(":") && !keyReplacementNameVar.endsWith(":")) {
                String[] arrayKeyReplacementWithDefaultValue = keyReplacementNameVar.split(":");
                keyReplacementNameVar = arrayKeyReplacementWithDefaultValue[0];
                defaultValue = arrayKeyReplacementWithDefaultValue[1];
            }

            if (keyReplacementNameVar.equals(nameVarToAvoidRecursion))
                throw new IllegalArgumentException(
                        String.format("var %s has a value that requires itself", nameVarToAvoidRecursion));

            String valueFind = (String) mapVars.get(keyReplacementNameVar);

            if (valueFind == null) {
                valueFind = System.getenv(keyReplacementNameVar);
                if (valueFind != null) {
                    valueFind = (String) handleDeclarationsInValues(mapVars, valueFind, keyReplacementNameVar);
                }
            }

            if (valueFind == null) {
                valueFind = defaultValue;
            }
            value = value.replace(keyReplacementGroup, valueFind);
            value = value.trim().replaceAll("^\"", "").replaceAll("\"$", "");

        }
        return value;
    }

    public static void handleDeclarationsInMap(final Map<String, Object> mapVars) {
        Map<String, Object> mapVarWithNormalizedKey = mapVars
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> normalizeNameVar(entry.getKey()),
                        Map.Entry::getValue));
        mapVars.clear();
        mapVars.putAll(mapVarWithNormalizedKey);
        mapVars.entrySet().forEach(
                entry -> entry.setValue(handleDeclarationsInValues(mapVars, entry.getValue(), entry.getKey())));
    }

    public static Collector<String, List<String>, Map<String, Object>> collectVarsToMap(final String delimiter,
            final String prefixVar) {

        Objects.requireNonNull(delimiter);
        Objects.requireNonNull(prefixVar);

        String patterString = String.format("^%s.*%s.*", prefixVar, delimiter);

        return new Collector<String, List<String>, Map<String, Object>>() {
            @Override
            public Supplier<List<String>> supplier() {
                return ArrayList::new;
            }

            @Override
            public BiConsumer<List<String>, String> accumulator() {
                return (list, stringItem) -> {
                    if (stringItem != null && stringItem.matches(patterString)) {
                        String stringItemWithNoPrefix = Objects.equals(prefixVar, "")
                                ? stringItem
                                : stringItem.replaceFirst(prefixVar, "");

                        String[] keyAndValueArray = stringItemWithNoPrefix.split(delimiter);

                        String stringItemNormalized = String.format("%s%s%s",
                                VarCollectorUtil.normalizeNameVar(keyAndValueArray[0]),
                                delimiter,
                                keyAndValueArray[1] != null
                                        ? keyAndValueArray[1].trim().replaceAll("^\"", "").replaceAll("\"$", "")
                                        : "");
                        list.add(stringItemNormalized);
                    }
                };
            }

            @Override
            public BinaryOperator<List<String>> combiner() {
                return (list1, list2) -> {
                    list1.addAll(list2);
                    return list1;
                };
            }

            @Override
            public Function<List<String>, Map<String, Object>> finisher() {
                return list -> list.stream()
                        .collect(Collectors.toMap(s -> s.split(delimiter)[0], s -> s.split(delimiter)[1]));
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Collections.singleton(Characteristics.UNORDERED);
            }
        };
    }

}
