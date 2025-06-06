package com.eshore.dm.cdc.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;

/**
 * @Author: zhuzhibin
 * @Email:
 * @Date: 2025/6/5 16:21
 * @Description: TODO
 */
public class StringUtils {
    public static String hex2String(String hexString) {
        byte[] decodedBytes;
        try {
            decodedBytes = Hex.decodeHex(hexString.toCharArray());
            String result = new String(decodedBytes, StandardCharsets.UTF_8);
            return result;
        } catch (DecoderException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String cleanColumn(String column) {
        if (column == null) {
            return null;
        }
        if (column.contains("`")) {
            column = column.replaceAll("`", "");
        }

        if (column.contains("'")) {
            column = column.replaceAll("'", "");
        }

        if (column.contains("\"")) {
            column = column.replaceAll("\"", "");
        }

        return column;
    }
}
