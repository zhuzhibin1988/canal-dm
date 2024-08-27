import io.debezium.connector.dameng.DamengConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DamengTest {

    private static DebeziumEngine<ChangeEvent<String, String>> engine;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("name", "dameng-engine-0036"); // 唯一实例
        props.setProperty("connector.class", DamengConnector.class.getName());
        props.setProperty("offset.storage", FileOffsetBackingStore.class.getName());
        // 指定 offset 存储目录
        props.setProperty("offset.storage.file.filename", "E:\\program\\java\\debezium-dameng\\offset.txt");
        // 指定 Topic offset 写入磁盘的间隔时间
        props.setProperty("offset.flush.interval.ms", "60000");
        // 设置数据库连接信息
        props.setProperty("database.hostname", "192.168.137.1");
        props.setProperty("database.port", "5236");
        props.setProperty("database.user", "SYSDBA");
        props.setProperty("database.password", "SYSDBA");
        props.setProperty("database.server.id", "85701");

        props.setProperty("table.include.list", "DEV.T_DATA");
        // props.setProperty("snapshot.include.collection.list", "TEST");
        props.setProperty("database.history", FileDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.file.filename", "E:\\program\\java\\debezium-dameng\\history.txt");
        // 每次运行需要对此参数进行修改，因为此参数唯一
        props.setProperty("database.server.name", "my-dameng-connector-2022-0926-1448");
        // 指定 CDB 模式的实例名
        props.setProperty("database.dbname", "SYSDBA");
        // 是否输出 schema 信息
        props.setProperty("key.converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");
        props.setProperty("database.serverTimezone", "UTC"); // 时区
        props.setProperty("database.connection.adapter", "logminer"); // 模式


        props.setProperty("debezium.log.mining.strategy", "online_catalog");
        props.setProperty("debezium.log.mining.continuous.mine", "true");
        // 使用上述配置创建Debezium引擎，输出样式为Json字符串格式
        engine =
                DebeziumEngine.create(Json.class)
                        .using(props)
                        .notifying(
                                record -> {
                                    // record中会有操作的类型（增、删、改）和具体的数据
                                    System.out.println(record);
                                    //                    System.out.println("record.key() = " + record.key());
                                    //                    System.out.println("record.value() = " + record.value());
                                })
                        .using(
                                (success, message, error) -> {
                                    // 查看错误信息
                                    if (!success && error != null) {
                                        // 报错回调
                                        System.out.println("----------error------");
                                        System.out.println(message);
                                        System.out.println(error);
                                        error.printStackTrace();
                                    }
                                    closeEngine(engine);
                                })
                        .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        //addShutdownHook(engine);
        //awaitTermination(executor);

        //System.out.println("------------main finished.");
    }

    private static void closeEngine(DebeziumEngine<ChangeEvent<String, String>> engine) {
        try {
            engine.close();
        } catch (IOException ignored) {
        }
    }

    private static void addShutdownHook(DebeziumEngine<ChangeEvent<String, String>> engine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> closeEngine(engine)));
    }

    private static void awaitTermination(ExecutorService executor) {
        if (executor != null) {
            try {
                executor.shutdown();
                while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
