package com.moniepoint.kvstore.comparator;

import java.util.Comparator;

public class AlphanumericComparator implements Comparator<String> {
    @Override
    public int compare(String s1, String s2) {
        int i1 = 0, i2 = 0;
        int n1 = s1.length(), n2 = s2.length();

        while (i1 < n1 && i2 < n2) {
            char c1 = s1.charAt(i1);
            char c2 = s2.charAt(i2);

            if (Character.isDigit(c1) && Character.isDigit(c2)) {
                long num1 = 0;
                while (i1 < n1 && Character.isDigit(s1.charAt(i1))) {
                    num1 = num1 * 10 + (s1.charAt(i1) - '0');
                    i1++;
                }

                long num2 = 0;
                while (i2 < n2 && Character.isDigit(s2.charAt(i2))) {
                    num2 = num2 * 10 + (s2.charAt(i2) - '0');
                    i2++;
                }

                if (num1 != num2) {
                    return Long.compare(num1, num2);
                }
            } else {
                if (c1 != c2) {
                    return c1 - c2;
                }
                i1++;
                i2++;
            }
        }

        return n1 - n2;
    }
}
