package com.taobao.tddl.dbsync.binlog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import com.jtljia.pump.canal.parse.driver.mysql.MysqlQueryExecutor;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.jtljia.pump.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.jtljia.pump.exception.ParseException;
import com.taobao.tddl.dbsync.binlog.TableMeta.FieldMeta;

/**
 * 处理table meta解析和缓存
 */
public class TableMetaCache {

    public static final String     COLUMN_NAME    = "COLUMN_NAME";
    public static final String     COLUMN_TYPE    = "COLUMN_TYPE";
    public static final String     IS_NULLABLE    = "IS_NULLABLE";
    public static final String     COLUMN_KEY     = "COLUMN_KEY";
    public static final String     COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String     EXTRA          = "EXTRA";
    private MysqlQueryExecutor     queryExecutor;

    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
    private Map<String, TableMeta> tableMetaCache;

    public TableMetaCache(MysqlQueryExecutor con){
        this.queryExecutor = con;
        tableMetaCache = new ConcurrentHashMap<String, TableMeta>();
    }

    public TableMeta getTableMeta(String schema, String table) {
        return getTableMeta(schema, table, true);
    }

    public TableMeta getTableMeta(String schema, String table, boolean useCache) {
        if (!useCache) {
            tableMetaCache.remove(getFullName(schema, table));
        }

        String tableFullName = getFullName(schema, table);
        if( !tableMetaCache.containsKey(tableFullName) ) {
        	try {
                return getTableMeta0(tableFullName);
            } catch (IOException e) {
                // 尝试做一次retry操作
                try {
                	queryExecutor.reconnect();
                    return getTableMeta0(tableFullName);
                } catch (IOException e1) {
                    throw new ParseException("fetch failed by table meta:" + tableFullName, e1);
                }
            }
        }
        return tableMetaCache.get( tableFullName );
    }

    public void clearTableMeta(String schema, String table) {
        tableMetaCache.remove(getFullName(schema, table));
    }

    public void clearTableMetaWithSchemaName(String schema) {
        // Set<String> removeNames = new HashSet<String>(); //
        // 存一份临时变量，避免在遍历的时候进行删除
        for (String name : tableMetaCache.keySet()) {
            if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
                // removeNames.add(name);
                tableMetaCache.remove(name);
            }
        }
    }

    public void clearTableMeta() {
        tableMetaCache.clear();
    }

    private TableMeta getTableMeta0(String fullname) throws IOException {
        ResultSetPacket packet = queryExecutor.query("desc " + fullname);
        return new TableMeta(fullname, parserTableMeta(packet));
    }

    private List<FieldMeta> parserTableMeta(ResultSetPacket packet) {
        Map<String, Integer> nameMaps = new HashMap<String, Integer>(6, 1f);

        int index = 0;
        for (FieldPacket fieldPacket : packet.getFieldDescriptors()) {
            nameMaps.put(fieldPacket.getOriginalName(), index++);
        }

        int size = packet.getFieldDescriptors().size();
        int count = packet.getFieldValues().size() / packet.getFieldDescriptors().size();
        List<FieldMeta> result = new ArrayList<FieldMeta>();
        for (int i = 0; i < count; i++) {
            FieldMeta meta = new FieldMeta();
            // 做一个优化，使用String.intern()，共享String对象，减少内存使用
            meta.setColumnName(packet.getFieldValues().get(nameMaps.get(COLUMN_NAME) + i * size).intern());
            meta.setColumnType(packet.getFieldValues().get(nameMaps.get(COLUMN_TYPE) + i * size));
            meta.setIsNullable(packet.getFieldValues().get(nameMaps.get(IS_NULLABLE) + i * size));
            meta.setIskey(packet.getFieldValues().get(nameMaps.get(COLUMN_KEY) + i * size));
            meta.setDefaultValue(packet.getFieldValues().get(nameMaps.get(COLUMN_DEFAULT) + i * size));
            meta.setExtra(packet.getFieldValues().get(nameMaps.get(EXTRA) + i * size));

            result.add(meta);
        }

        return result;
    }

    private String getFullName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        return builder.append('`').append(schema).append('`')
            .append('.').append('`').append(table).append('`')
            .toString();
    }
}
