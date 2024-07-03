package org.example;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.springframework.boot.*;
import org.example.producer;

import java.sql.*;


// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {

    }
}













/*
        *
        * try{
        data_connector data_pool = new data_connector();
        producer tech = new producer("technology", data_pool);
        boolean shut_off = false;

        while (!shut_off) {

            PGNotification[] notifications = tech.getConn().unwrap(PGConnection.class).getNotifications();

            // TODO
            // Send data from producer through kafka after splitting into chunks
            // Send as json data
            // Create threads and configure operations for multithreading topics
            // Create ACK + topics topic in kafka
            // Create event loop in python waiting for ACK

            for (PGNotification notification : notifications) {
                System.out.println("Received notification:");
                if (notification.getName().equals("batch_added")){
                    tech.collect_data();
                }

            }


            Thread.sleep(1000);
            //shut_off = true;
        }
    } catch (SQLException | InterruptedException e) {
        e.printStackTrace();
    }
        System.out.println("Exited test case");


        *
        * */
// Close the connection