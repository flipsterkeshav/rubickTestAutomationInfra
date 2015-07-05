package tests.engage.engage_api.rubick.DAO;

import com.flipkart.w3.notifications.dblayer.exceptions.HBaseException;
import com.flipkart.w3.notifications.dblayer.objects.HbaseColumn;
import com.flipkart.w3.notifications.dblayer.objects.HbaseRow;
import com.flipkart.w3.notifications.dblayer.util.Validator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.*;

import static com.flipkart.website.testng.Logger.log;

/**
 * Created by keshav.gupta on 20/05/15.
 */
public class HBaseClient {

    private HTablePool hbasePool;

    public HBaseClient() {
        Configuration config=getConfigHadoopServer();
        hbasePool=new HTablePool(config, 20);
    }

    private byte[] toB(String s)
    {
        return Bytes.toBytes(s);
    }

    private byte[] toB(Object s) {

        if(s instanceof String)
            return Bytes.toBytes((String)s);
        if(s instanceof Long)
            return Bytes.toBytes((Long)s);
        if(s instanceof Integer)
            return Bytes.toBytes((Integer)s);

        return null;
    }

    private String toS(byte[] b)
    {
        return new String(b);
    }

    public Configuration getConfigHadoopServer() {

        Map<String, String> map = new HashMap<String, String>();

        /*
        donot require kerberos authentication as of now
        map.put("hbase.zookeeper.quorum","hedwig-hbasenn-int-0001.nm.flipkart.com,hedwig-hbasenn-int-0002.nm.flipkart.com,hedwig-hbasedn-int-0001.nm.flipkart.com");
        map.put("hadoop.security.authentication", "none");
        map.put("hbase.master.kerberos.principal", "");
        map.put("hbase.regionserver.kerberos.principal", "");
        */

        map.put("hbase.rootdir", "hdfs://hedwig/hbase");
        map.put("hbase.master.port", "60000");
        map.put("hbase.zookeeper.quorum", "hedwig-hbasenn-int-0001.nm.flipkart.com");
        map.put("hbase.regionserver.lease.period", "2000");
        map.put("hbase.rpc.timeout", "2000");
        map.put("hbase.rpc.engine", "");
        map.put("hbase.table.max.references", "20");
        map.put("hbase.client.retries.number", "1");
        Configuration config = HBaseConfiguration.create();
        for (Map.Entry<String, String> configuration : map.entrySet()) {
            if (configuration.getValue() != null && !(configuration.getValue().isEmpty())) {
                config.set(configuration.getKey(), configuration.getValue());
            }
        }
        UserGroupInformation.setConfiguration(config);
        return config;
    }

    public Map<String, Map<String, HbaseColumn>> fetchDbRowsForUserId(String table,String userId) throws Exception {
        return scanRangeData(table,"a",userId,userId + String.valueOf(Character.MAX_VALUE));
    }
    public void deleteAllRowsForUserId(String table,String userId) throws Exception {
        HTableInterface hTable=hbasePool.getTable("rubick.actions");
        Map<String, Map<String, HbaseColumn>> rows=scanRangeData(table, "a", userId, userId + String.valueOf(Character.MAX_VALUE));
        Delete delete;
        Iterator rowI = rows.entrySet().iterator();
        Map.Entry row;
        while(rowI.hasNext()) {
            row = (Map.Entry)rowI.next();
            delete=delete=new Delete(toB(row.getKey()));
            System.out.println(row.getKey());
            hTable.delete(delete);
        }
    }


    public void insertRows(String tableName, List<HbaseRow> rows) throws HBaseException {
        if(!Validator.isEmpty(tableName) && !Validator.isEmpty(rows)) {
            ArrayList puts = new ArrayList();
            Iterator table = rows.iterator();

            while(table.hasNext()) {
                HbaseRow e = (HbaseRow)table.next();
                puts.add(this.rowToPut(e));
            }

            HTableInterface table1;
            try {
                table1 = this.hbasePool.getTable(tableName);
            } catch (Exception var16) {
                throw new HBaseException(var16);
            }

            try {
                table1.put(puts);
            } catch (IOException var14) {
                throw new HBaseException(var14);
            } finally {
                try {
                    table1.close();
                } catch (IOException var13) {
                    logWrapper("Exception occurred while closing hbase table connection");
                }

            }

        } else {
            throw new HBaseException("Improper arguments passed to HbaseClient:insertRows");
        }
    }

    public List<HbaseRow> getHbaseActionRows( List<Action> actions ) {
        List<HbaseRow> rows = Lists.newArrayList();

        for (Action action : actions ) {
            if ( action != null ) {
                HbaseRow row = new HbaseRow(getHbaseKeyForRow(action), "a", action.getAttributeMap());
                rows.add(row);
            }
        }

        return rows;
    }

    private String getHbaseKeyForRow( Action action ) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(action.getAttributeValue("userId")).append(":")
                .append(action.getActionId()).append(":").append(action.getTimestamp());

        return stringBuilder.toString();
    }


    private Put rowToPut(HbaseRow row) {
        if(row == null) {
            throw new NullPointerException("Row argument cannot be null");
        } else {
            Put put = new Put(this.toB(row.getKey()));
            String columnFamily = row.getColumnFamiliy();
            if(row.getKeyvalues() != null && row.getKeyvalues().size() > 0) {
                Iterator i$ = row.getKeyvalues().entrySet().iterator();

                while(i$.hasNext()) {
                    Map.Entry kv = (Map.Entry)i$.next();
                    if(!Validator.isEmpty(kv.getKey()) && !Validator.isEmpty(kv.getValue())) {
                        put.add(this.toB(columnFamily), this.toB((String)kv.getKey()), this.toB((String)kv.getValue()));
                    }
                }
            } else {
                put.add(this.toB(columnFamily), this.toB("__DEFAULT_COLUMN__"), new byte[0]);
            }

            return put;
        }
    }


    public void logWrapper(String log) {
        System.out.println(log+"\n");
        log(log);

    }

    public Map<String, Map<String, HbaseColumn>> scanRangeData(String htable,String columnFamily,String startRow,String stopRow) throws Exception {
        HTableInterface hTable=hbasePool.getTable(htable);
        Scan scan = new Scan(toB(startRow),toB(stopRow));
        ResultScanner scanner;
        try {
            scanner = hTable.getScanner(scan);
            scan.setCaching(1000);
        } catch (IOException e) {
            throw new Exception(e);
        }
        Map<String, Map<String, HbaseColumn>> rows = new HashMap<String, Map<String, HbaseColumn>>();
        try {
            for(Result result = scanner.next(); result!=null; result=scanner.next()) {
                rows.put(new String(result.getRow()),getRowFromResult(result,columnFamily));
            }
        } catch (IOException e) {
            throw new Exception(e);
        } finally {
            scanner.close();
            hTable.flushCommits();
            hTable.close();
        }
        return rows;
    }

    public Map<String, Map<String, String>> getKVMapRowsFromRows(Map<String, Map<String, HbaseColumn>> hbaseRows) {
        if(Validator.isEmpty(hbaseRows)) {
            return new HashMap();
        } else {
            LinkedHashMap rows = new LinkedHashMap();
            Iterator i$ = hbaseRows.entrySet().iterator();

            while(i$.hasNext()) {
                Map.Entry row = (Map.Entry)i$.next();
                rows.put(row.getKey(), this.getKVMapFromRow((Map)row.getValue()));
            }

            return rows;
        }
    }

    private Map<String, String> getKVMapFromRow(Map<String, HbaseColumn> hbaseRow) {
        if(Validator.isEmpty(hbaseRow)) {
            return new HashMap();
        } else {
            HashMap kv = Maps.newHashMap();
            Iterator i$ = hbaseRow.entrySet().iterator();

            while(i$.hasNext()) {
                Map.Entry column = (Map.Entry)i$.next();
                kv.put(column.getKey(), ((HbaseColumn)column.getValue()).getValue());
            }

            return kv;
        }
    }

    private Map<String, HbaseColumn> getRowFromResult(Result result, String columnFamily) {
        Map<String, HbaseColumn> columns = new HashMap<String, HbaseColumn>();

        if(Validator.isEmpty(result)){
            return columns;
        }

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> data =result.getMap();
        if(Validator.isEmpty(data)){
            return columns;
        }
        NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifiers = data.get(toB(columnFamily));
        for(byte[] qualifier: qualifiers.keySet())
        {
            KeyValue kv =result.getColumnLatest(toB(columnFamily),qualifier);
            HbaseColumn column = new HbaseColumn(toS(qualifier), toS(kv.getValue()), kv.getTimestamp());
            columns.put(toS(qualifier),column);
        }
        return columns;
    }

}
