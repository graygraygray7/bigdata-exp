package sdu.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 实验三 第一部分：熟悉常用的 HBase 操作（Java API 实现）。
 *
 * <p>实现题目要求的 5 个方法：
 * createTable / addRecord / scanColumn / modifyData / deleteRow，
 * 并额外实现与 HBase Shell 对应的：列出所有表、显示整表、增删列族/列、清空表、统计行数。</p>
 *
 * <p>表结构设计（把关系型三张表映射到 HBase）：
 * <ul>
 *   <li>student：row = S_Name，列族 info，列限定符 S_No / S_Sex / S_Age；</li>
 *   <li>course ：row = C_No，列族 info，列限定符 C_Name / C_Credit；</li>
 *   <li>sc     ：row = S_Name，列族 Score，列限定符 = 课程名（Math / Computer Science / English），
 *       值 = 成绩。这正是题目 addRecord 示例中 "Score:Math" 的形式。</li>
 * </ul>
 * 选课表按学生姓名聚合到同一行，是 HBase 常见的反规范化设计：一次 scan 就能取出某学生的全部成绩。</p>
 */
public class HBaseDemo implements AutoCloseable {

    private final Connection connection;
    private final Admin admin;

    public HBaseDemo() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(conf);
        this.admin = connection.getAdmin();
    }

    // ==================================================================
    // 题目要求的 5 个方法
    // ==================================================================

    /**
     * 创建表。fields 的每个元素是一个列族名；若表已存在，先禁用并删除再创建。
     */
    public void createTable(String tableName, String[] fields) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            if (admin.isTableEnabled(tn)) {
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
            System.out.println("[createTable] 表 " + tableName + " 已存在，已删除旧表");
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
        for (String field : fields) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(field));
        }
        admin.createTable(builder.build());
        System.out.println("[createTable] 已创建表 " + tableName + "，列族 " + String.join(", ", fields));
    }

    /**
     * 向指定行写入若干单元格。fields 元素形如 "列族" 或 "列族:列限定符"。
     */
    public void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        if (fields.length != values.length) {
            throw new IllegalArgumentException("fields 与 values 长度不一致");
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(row));
            for (int i = 0; i < fields.length; i++) {
                String[] fq = fields[i].split(":", 2);
                byte[] family = Bytes.toBytes(fq[0]);
                byte[] qualifier = fq.length == 2 ? Bytes.toBytes(fq[1]) : new byte[0];
                put.addColumn(family, qualifier, Bytes.toBytes(values[i]));
            }
            table.put(put);
        }
        System.out.println("[addRecord] " + tableName + " / " + row + " 写入 " + fields.length + " 个单元格");
    }

    /**
     * 浏览某一列的数据。column 为列族名时列出该列族下所有列限定符；为 "列族:列" 时只列该列。
     * 某行没有该列时输出 null。
     */
    public void scanColumn(String tableName, String column) throws IOException {
        String family = column;
        String qualifier = null;
        int idx = column.indexOf(':');
        if (idx > 0) {
            family = column.substring(0, idx);
            qualifier = column.substring(idx + 1);
        }
        System.out.println("\n[scanColumn] 表 " + tableName + " 的列 " + column);
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result result : scanner) {
                String row = Bytes.toString(result.getRow());
                boolean found = false;
                List<Cell> cells = result.listCells();
                if (cells != null) {
                    for (Cell cell : cells) {
                        String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                        String cq = Bytes.toString(CellUtil.cloneQualifier(cell));
                        if (!cf.equals(family)) {
                            continue;
                        }
                        if (qualifier != null && !cq.equals(qualifier)) {
                            continue;
                        }
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.println("  " + row + "\t" + cf + ":" + cq + " = " + value);
                        found = true;
                    }
                }
                if (!found) {
                    System.out.println("  " + row + "\t" + column + " = null");
                }
            }
        }
    }

    /**
     * 修改指定单元格的值（题目签名）。新值从标准输入读取，实际写入委托给 4 参重载。
     */
    public void modifyData(String tableName, String row, String column) throws IOException {
        System.out.print("[modifyData] 请输入 " + tableName + " / " + row + " / " + column + " 的新值: ");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String value = reader.readLine();
        modifyData(tableName, row, column, value);
    }

    /**
     * 修改指定单元格的值（便于程序调用的重载）。
     */
    public void modifyData(String tableName, String row, String column, String value) throws IOException {
        String[] fq = splitColumn(column);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(fq[0]), Bytes.toBytes(fq[1]), Bytes.toBytes(value));
            table.put(put);
        }
        System.out.println("[modifyData] " + tableName + " / " + row + " / " + column + " 已修改为 " + value);
    }

    /**
     * 删除指定行。
     */
    public void deleteRow(String tableName, String row) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.delete(new Delete(Bytes.toBytes(row)));
        }
        System.out.println("[deleteRow] 已删除 " + tableName + " 中的行 " + row);
    }

    // ==================================================================
    // 与 HBase Shell 对应的 5 类操作
    // ==================================================================

    /** 列出所有表及其列族、启用状态。 */
    public void listTables() throws IOException {
        System.out.println("\n[listTables] HBase 中的所有表：");
        List<TableDescriptor> descriptors = admin.listTableDescriptors();
        if (descriptors.isEmpty()) {
            System.out.println("  (无表)");
            return;
        }
        for (TableDescriptor td : descriptors) {
            List<String> families = new ArrayList<>();
            for (byte[] family : td.getColumnFamilyNames()) {
                families.add(Bytes.toString(family));
            }
            System.out.println("  " + td.getTableName().getNameAsString()
                    + "\tenabled=" + admin.isTableEnabled(td.getTableName())
                    + "\tfamilies=" + families);
        }
    }

    /** 显示整张表的所有记录。 */
    public void scanTable(String tableName) throws IOException {
        System.out.println("\n[scanTable] 表 " + tableName + " 的全部记录：");
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(new Scan())) {
            int count = 0;
            for (Result result : scanner) {
                StringBuilder sb = new StringBuilder("  " + Bytes.toString(result.getRow()) + " -> ");
                List<Cell> cells = result.listCells();
                if (cells != null) {
                    boolean first = true;
                    for (Cell cell : cells) {
                        if (!first) {
                            sb.append(", ");
                        }
                        first = false;
                        sb.append(Bytes.toString(CellUtil.cloneFamily(cell)))
                                .append(':')
                                .append(Bytes.toString(CellUtil.cloneQualifier(cell)))
                                .append('=')
                                .append(Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
                System.out.println(sb);
                count++;
            }
            System.out.println("  共 " + count + " 行");
        }
    }

    /** 增加列族。 */
    public void addColumnFamily(String tableName, String family) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(family);
        admin.addColumnFamily(tn, cfd);
        System.out.println("[addColumnFamily] " + tableName + " 已增加列族 " + family);
    }

    /** 删除列族（先禁用表，删除后重新启用）。 */
    public void deleteColumnFamily(String tableName, String family) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        admin.disableTable(tn);
        admin.deleteColumnFamily(tn, Bytes.toBytes(family));
        admin.enableTable(tn);
        System.out.println("[deleteColumnFamily] " + tableName + " 已删除列族 " + family);
    }

    /** 删除某行某列（保留该行其它列）。 */
    public void deleteColumn(String tableName, String row, String column) throws IOException {
        String[] fq = splitColumn(column);
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(Bytes.toBytes(row));
            delete.addColumns(Bytes.toBytes(fq[0]), Bytes.toBytes(fq[1]));
            table.delete(delete);
        }
        System.out.println("[deleteColumn] 已删除 " + tableName + " / " + row + " / " + column);
    }

    /** 清空表中所有记录（保留表结构与列族）。 */
    public void truncateTable(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.isTableEnabled(tn)) {
            admin.disableTable(tn);
        }
        // truncateTable 内部会在完成后自动重新启用表
        admin.truncateTable(tn, false);
        System.out.println("[truncateTable] 已清空表 " + tableName);
    }

    /** 统计表行数。 */
    public long countRows(String tableName) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName));
             ResultScanner scanner = table.getScanner(new Scan())) {
            long count = 0;
            for (Result ignored : scanner) {
                count++;
            }
            System.out.println("[countRows] 表 " + tableName + " 共 " + count + " 行");
            return count;
        }
    }

    // ==================================================================
    // 演示流程
    // ==================================================================

    private void run() throws IOException {
        System.out.println("========== 1. 列出 HBase 中的所有表 ==========");
        listTables();

        System.out.println("\n========== 2. 创建 student / course / sc 三张表 ==========");
        createTable("student", new String[]{"info"});
        createTable("course", new String[]{"info"});
        createTable("sc", new String[]{"Score"});

        System.out.println("\n========== 3. 插入关系表中的数据 ==========");
        // 学生表：row = 姓名
        addRecord("student", "Zhangsan",
                new String[]{"info:S_No", "info:S_Sex", "info:S_Age"},
                new String[]{"2015001", "male", "23"});
        addRecord("student", "Mary",
                new String[]{"info:S_No", "info:S_Sex", "info:S_Age"},
                new String[]{"2015003", "female", "22"});
        addRecord("student", "Lisi",
                new String[]{"info:S_No", "info:S_Sex", "info:S_Age"},
                new String[]{"2015003", "male", "24"});

        // 课程表：row = 课程号
        addRecord("course", "123001",
                new String[]{"info:C_Name", "info:C_Credit"},
                new String[]{"Math", "2.0"});
        addRecord("course", "123002",
                new String[]{"info:C_Name", "info:C_Credit"},
                new String[]{"Computer Science", "5.0"});
        addRecord("course", "123003",
                new String[]{"info:C_Name", "info:C_Credit"},
                new String[]{"English", "3.0"});

        // 选课表：row = 姓名，列族 Score，列 = 课程名
        addRecord("sc", "Zhangsan",
                new String[]{"Score:Math", "Score:English"},
                new String[]{"86", "69"});
        addRecord("sc", "Mary",
                new String[]{"Score:Computer Science", "Score:English"},
                new String[]{"77", "99"});
        addRecord("sc", "Lisi",
                new String[]{"Score:Math", "Score:Computer Science"},
                new String[]{"98", "95"});

        System.out.println("\n========== 4. 显示整表记录 ==========");
        scanTable("student");
        scanTable("course");
        scanTable("sc");

        System.out.println("\n========== 5. 按列族 / 列浏览 ==========");
        scanColumn("student", "info");
        scanColumn("sc", "Score");
        scanColumn("sc", "Score:Math");

        System.out.println("\n========== 6. 修改单元格：Zhangsan 的 Math 86 -> 95 ==========");
        modifyData("sc", "Zhangsan", "Score:Math", "95");
        scanColumn("sc", "Score:Math");

        System.out.println("\n========== 7. 删除行：删除 Lisi 的选课记录 ==========");
        countRows("sc");
        deleteRow("sc", "Lisi");
        countRows("sc");
        // 恢复数据，便于后续 shell 对照
        addRecord("sc", "Lisi",
                new String[]{"Score:Math", "Score:Computer Science"},
                new String[]{"98", "95"});

        System.out.println("\n========== 8. 增加 / 删除列族与列 ==========");
        addColumnFamily("sc", "Remark");
        addRecord("sc", "Zhangsan", new String[]{"Remark:Note"}, new String[]{"monitor"});
        scanColumn("sc", "Remark");
        deleteColumn("sc", "Zhangsan", "Remark:Note");
        scanColumn("sc", "Remark");
        deleteColumnFamily("sc", "Remark");

        System.out.println("\n========== 9. 清空表：以临时表 tmp_truncate 演示 ==========");
        createTable("tmp_truncate", new String[]{"info"});
        addRecord("tmp_truncate", "r1", new String[]{"info:v"}, new String[]{"1"});
        addRecord("tmp_truncate", "r2", new String[]{"info:v"}, new String[]{"2"});
        countRows("tmp_truncate");
        truncateTable("tmp_truncate");
        countRows("tmp_truncate");
        admin.disableTable(TableName.valueOf("tmp_truncate"));
        admin.deleteTable(TableName.valueOf("tmp_truncate"));
        System.out.println("[cleanup] 已删除临时表 tmp_truncate");

        System.out.println("\n========== 10. 最终状态 ==========");
        listTables();
        countRows("student");
        countRows("course");
        countRows("sc");
        System.out.println("\n✅ HBase Java API 演示完成");
    }

    private static String[] splitColumn(String column) {
        int idx = column.indexOf(':');
        if (idx <= 0) {
            throw new IllegalArgumentException("列名必须形如 列族:列限定符，实际为 " + column);
        }
        return new String[]{column.substring(0, idx), column.substring(idx + 1)};
    }

    @Override
    public void close() throws IOException {
        admin.close();
        connection.close();
    }

    public static void main(String[] args) {
        try (HBaseDemo demo = new HBaseDemo()) {
            demo.run();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
