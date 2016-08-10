package com.mapbar.execmysqlscript;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author Administrator
 * @date 2016-06-15
 * @modify
 */

public class ExecMysqlScript {

    private static final Logger log = LoggerFactory.getLogger(ExecMysqlScript.class);

    public static void main(String[] args) {
        exec();
    }

    private static void exec() {
        Properties prop = new Properties();
        try {
            File directory = new File(ExecMysqlScript.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getParentFile();
            log.info("config path: " + directory.getAbsolutePath() + "/config/config.properties");
            prop.load(new FileInputStream(directory.getAbsolutePath() + "/config/config.properties"));
        } catch (URISyntaxException e) {
            log.error("", e);
            return;
        } catch (IOException e) {
            log.error("", e);
            return;
        }

        String driver = prop.getProperty("driver");
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");
        String sqlpath = prop.getProperty("sqlpath");


        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            log.error("", e);
            return;
        } catch (SQLException e) {
            log.error("", e);
            return;
        }

        FluentIterable<File> iterable = Files.fileTreeTraverser().breadthFirstTraversal(new File(sqlpath)).filter(new Predicate<File>() {
            public boolean apply(File input) {
                return input.isFile();
            }
        });
        Stopwatch all = Stopwatch.createStarted();
        try {
            log.info("开始导入sql文件");
            for (File f : iterable) {
                log.info(f.getAbsolutePath());
                Stopwatch watch = Stopwatch.createStarted();
                String sqlString = Files.toString(f, Charsets.UTF_8);
                Iterable<String> sql = Splitter.on(";").split(sqlString);
                for (String s : sql) {
                    log.info(s);
                    stmt = conn.createStatement();
                    stmt.executeUpdate(s);
                }
                log.info(String.format(f.getName() + " file import completed in %d milliseconds.", watch.elapsed(TimeUnit.MILLISECONDS)));
            }
            log.info(String.format("all sql file import completed in %d milliseconds.", all.elapsed(TimeUnit.MILLISECONDS)));
            stmt.close();
            conn.close();
        } catch (IOException e) {
            log.error("", e);
        } catch (SQLException e) {
            log.error("", e);
        }
    }
}
