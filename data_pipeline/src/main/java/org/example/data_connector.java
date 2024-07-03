package org.example;

import java.sql.Connection;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.dbcp2.PoolingDataSource;
import java.sql.SQLException;

public class data_connector {




    private final PoolingDataSource<PoolableConnection> dataSource;

    public data_connector(){
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(jdbcUrl, username, password);
        PoolableConnectionFactory poolFactory = new PoolableConnectionFactory(connectionFactory,null);
        GenericObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolFactory);
        poolFactory.setPool(connectionPool);
        this.dataSource =  new PoolingDataSource<>(connectionPool);
    }

    public Connection create_connection() throws SQLException  {

        return dataSource.getConnection();

    }


    public  void close_dataSource() throws SQLException {
        dataSource.close();
    }

}
