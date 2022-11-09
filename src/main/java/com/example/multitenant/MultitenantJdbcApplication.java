package com.example.multitenant;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.catalina.Server;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.servlet.function.HandlerFunction;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;

import javax.sql.DataSource;
import javax.xml.crypto.Data;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.springframework.web.servlet.function.RouterFunctions.route;

@SpringBootApplication
public class MultitenantJdbcApplication {

    public static void main(String[] args) {
        SpringApplication.run(MultitenantJdbcApplication.class, args);
    }

    @Bean
    RouterFunction<ServerResponse> routes(JdbcTemplate template) {
        return route()
                .GET("/customers", new HandlerFunction<ServerResponse>() {
                    @Override
                    public ServerResponse handle(ServerRequest request) throws Exception {
                        var results = template.query("select * from customer",
                                new RowMapper<DataConfiguration.Customer>() {
                                    @Override
                                    public DataConfiguration.Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
                                        return new DataConfiguration.Customer(rs.getInt("id"), rs.getString("name"));
                                    }
                                });
                        return ServerResponse.ok().body(results);
                    }
                })
                .build();
    }

}

@Configuration
class DataConfiguration {

    @Bean
    @Primary
    DataSource multitenantDataSource(Map<String, DataSource> dataSources) {

        var prefix = "ds";
        var map = dataSources
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        e -> (Object) Integer.parseInt(e.getKey().substring(prefix.length())),
                        e -> (Object) e.getValue()
                ));

        map.forEach((tenantId, ds) -> {
            var initializer = new ResourceDatabasePopulator(new ClassPathResource("schema.sql"),
                    new ClassPathResource(prefix + tenantId + "-data.sql"));

            initializer.execute((DataSource) ds);

            System.out.println("initialized datasource for " + tenantId);
        });

        var mds = new MultiTenantDataSource();
        mds.setTargetDataSources(map);
        return mds;
    }

    @Bean
    DataSource ds1() {
        return dataSource(5432);
    }

    @Bean
    DataSource ds2() {
        return dataSource(5431);
    }

    private static DataSource dataSource(int port) {
        var dsp = new DataSourceProperties();
        dsp.setPassword("psotgres"); //Terrible practice but whatever
        dsp.setPassword("postgres1"); //Terrible practice but whatever
        dsp.setUrl("jdbc:postgresql://localhost:" +
                "" + port + "/user");
        return dsp.initializeDataSourceBuilder() //Initialize datasource
                .type(HikariDataSource.class)
                .build();
    }

    class MultiTenantDataSource extends AbstractRoutingDataSource {

        private final AtomicBoolean initialized = new AtomicBoolean();

        @Override
        protected DataSource determineTargetDataSource() {
            if (this.initialized.compareAndSet(false, true)) {
                this.afterPropertiesSet();
            }
            return super.determineTargetDataSource();
        }

        @Override
        protected Object determineCurrentLookupKey() {
            var authentication = SecurityContextHolder.getContext().getAuthentication();
            return null;
        }
    }

    record Customer(Integer id, String name) {

    }
}