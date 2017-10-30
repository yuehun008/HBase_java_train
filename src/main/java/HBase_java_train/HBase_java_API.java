package HBase_java_train;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase_java_API {
    private static Configuration conf;
    private static Connection con;

    // 初始化连接
    static {
        conf = HBaseConfiguration.create(); // 获得配制文件对象
        conf.set("hbase.zookeeper.quorum", "192.168.137.12");
        try {
            con = ConnectionFactory.createConnection(conf);// 获得连接对象
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 获得连接
    public static Connection getCon() {
        if (con == null || con.isClosed()) {
            try {
                con = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return con;
    }

    // 关闭连接
    public static void close() {
        if (con != null) {
            try {
                con.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表
     */
    public static void creaatTable(String tableName, String... FamilyColumn) {
        TableName tn = TableName.valueOf(tableName);
        try {
            Admin admin = getCon().getAdmin();
            HTableDescriptor htd = new HTableDescriptor(tn);
            System.out.println("======准备创建 " + tableName + " 表======");
            if (admin.tableExists(tn)) {
                System.out.println("表 " + tableName + "存在");
            } else {
                for (String fc : FamilyColumn) {
                    HColumnDescriptor hcd = new HColumnDescriptor(fc);
                    htd.addFamily(hcd);
                }
                admin.createTable(htd);
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    /**
     * 删除表
     */
    public static void dropTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try {
            Admin admin = con.getAdmin();
            admin.disableTable(tn);
            admin.deleteTable(tn);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    //插入或更新数据

    /**
     * 插入或更新单个数据
     */
    public static boolean insert(String tableName, String rowKey,
                                 String family, String qualifier, String value) {
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            t.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return false;
    }

    //读取数据

    /**
     * 读取一个值
     */
    public static String byGet(String tableName, String rowKey, String family,
                               String qualifier) {
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result r = t.get(get);
            return Bytes.toString(CellUtil.cloneValue(r.listCells().get(0)));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return null;
    }

    /**
     * 取一个列族的值
     */
    public static Map <String, String> byGet(String tableName, String rowKey, String family) {
        Map <String, String> result = null;
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(Bytes.toBytes(family));
            Result r = t.get(get);
            List <Cell> cs = r.listCells();
            result = cs.size() > 0 ? new HashMap <String, String>() : result;
            for (Cell cell : cs) {
                result.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return result;
    }

    /**
     * 读取一行所有数据
     */
    public static Map <String, Map <String, String>> byGet(String tableName, String rowKey) {
        Map <String, Map <String, String>> results = null;
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result r = t.get(get);
            List <Cell> cs = r.listCells();
            results = cs.size() > 0 ? new HashMap <String, Map <String, String>>() : results;
            for (Cell cell : cs) {
                String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                if (results.get(familyName) == null) {
                    results.put(familyName, new HashMap <String, String>());
                }
                results.get(familyName).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return results;
    }

    //删除一个数据
    public static boolean del(String tableName, String rowKey, String family, String qualifier) {
        System.out.println("=========准备删除数据========");
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));

            if (qualifier != null) {
                del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            } else if (family != null) {
                del.addFamily(Bytes.toBytes(family));
            }
            //System.out.println(del);
            t.delete(del);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
        return false;
    }

    //删一行下一个列族数据
    public static boolean del(String tableName, String rowKey, String family) {
        return del(tableName, rowKey, family, null);
    }

    //删一行数据
    public static boolean del(String tableName, String rowKey) {
        return del(tableName, rowKey, null, null);
    }


    /**
     * 主入口
     */
    public static void main(String[] args) {
        //创建表
        try {
            /**
             * 测试创建表
             */
            //creaatTable("myTest01", "myfc1", "myfc2", "myfc3");
            //creaatTable("myTest02", "myfc1", "myfc2", "myfc3");

            /**
             * 测试删除表
             */
            //dropTable("myTest02");

            /**
             * 测试插入数据
             */
//            insert("myTest01","1","myfc1","sex","men");
//            insert("myTest01","1","myfc1","name","xiaoming");
//            insert("myTest01","1","myfc1","age","32");
//            insert("myTest01","1","myfc2","name","xiaohong");
//            insert("myTest01","1","myfc2","sex","woman");
//            insert("myTest01","1","myfc2","age","23");
            //更新一个数据
            //insert("myTest01","1","myfc1","age","999");
//            insert("myTest01","1","myfc3","name","wxk");
//            insert("myTest01","1","myfc3","sex","man");
//            insert("myTest01","1","myfc3","age","28");

            /**
             * 删数据
             */
            //删一个数据
            //del("myTest01","1","myfc3","age");
            //删一行下的列族数据
            //del("myTest01", "1", "myfc2");
            //删除一行数据
            //del("myTest01", "1");

            /**
             * 显示存在的表名
             */
            TableName[] result = getCon().getAdmin().listTableNames();
            System.out.println("=========打印存在的表名=======");
            for (TableName r1 : result) {
                System.out.println(r1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        /**
         * 测试读取单个值
         */
//        String cellresult = byGet("myTest01","1","myfc1","name");
//        System.out.println("结果是： "+cellresult);

        /**
         * 测试取出一个列族的值
         */
//        Map <String, String> rowfamilyresult = byGet("myTest01", "1", "myfc2");
//        System.out.println("结果是： "+ rowfamilyresult);

        /**
         * 测试取出一行数据所有值
         */
        Map <String, Map <String, String>> rowresult = byGet("myTest01", "1");
        //System.out.println("myTest01,rowkey=1 所有列族数据是： "+rowresult );
        System.out.println("========rowkey = 1 ========");
        for (Object key : rowresult.keySet()) {
            System.out.println("key = " + key + ", Value = " + rowresult.get(key));
        }


    }
//测试一下git是否有效！
    //再次测试！！！

}
