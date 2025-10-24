package com.moniepoint.kvstore.comparator;

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NaturalKeyComparator implements Comparator<String> {

    private static final Pattern KEY_PATTERN = Pattern.compile("([a-zA-Z]+)(\\d+)");

    @Override
    public int compare(String s1, String s2) {
        Matcher m1 = KEY_PATTERN.matcher(s1);
        Matcher m2 = KEY_PATTERN.matcher(s2);

        // If both match the pattern (e.g., "key1", "key10")
        if (m1.matches() && m2.matches()) {
            String text1 = m1.group(1);
            String num1 = m1.group(2);
            String text2 = m2.group(1);
            String num2 = m2.group(2);

            int textCompare = text1.compareTo(text2);
            if (textCompare != 0) {
                return textCompare;
            }

            // Compare numerical parts
            return Integer.compare(Integer.parseInt(num1), Integer.parseInt(num2));
        }

        // Fallback to default string comparison if pattern doesn't match
        return s1.compareTo(s2);
    }
}
