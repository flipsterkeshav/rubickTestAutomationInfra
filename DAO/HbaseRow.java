package tests.engage.engage_api.rubick.DAO;

/**
 * Created by keshav.gupta on 21/05/15.
 */

import com.flipkart.w3.notifications.dblayer.util.Validator;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by keshav.gupta on 21/05/15.
 */

public class HbaseRow {
    private String key;
    private String columnFamily;
    private Map<String, String> keyvalues;

    public HbaseRow(String key, String columnFamily, Map<String, String> keyvalues) {
        if(Validator.isEmpty(key)) {
            throw new IllegalArgumentException("Key cannot be empty");
        } else if(Validator.isEmpty(columnFamily)) {
            throw new IllegalArgumentException("Column family cannot be empty");
        } else {
            this.key = key;
            this.columnFamily = columnFamily;
            this.keyvalues = keyvalues;
        }
    }

    public String getKey() {
        return this.key;
    }

    public String getColumnFamiliy() {
        return this.columnFamily;
    }

    public Map<String, String> getKeyvalues() {
        return this.keyvalues;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("HbaseRow(key=%s, columnFamily=%s, keyvalues={ ", new Object[]{this.key, this.columnFamily}));
        Iterator i$ = this.keyvalues.entrySet().iterator();

        while(i$.hasNext()) {
            Map.Entry kv = (Map.Entry)i$.next();
            sb.append(String.format("%s=%s ", new Object[]{kv.getKey(), kv.getValue()}));
        }

        sb.append("})");
        return sb.toString();
    }
}

