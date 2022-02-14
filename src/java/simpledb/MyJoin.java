package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;
import simpledb.execution.*;
import java.io.*;

/*
SELECT *
        FROM some_data_file1,
        some_data_file2
        WHERE some_data_file1.field1 = some_data_file2.field1
        AND some_data_file1.id > 1
 */

public class MyJoin {

    public static void main(String[] argv) {
        // construct a 3-column table schema
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};

        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");



        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        // 1, 1, 1
        Tuple t1 = new Tuple(td);
        t1.setField(0, new IntField(1));
        t1.setField(1, new IntField(1));
        t1.setField(2, new IntField(1));
        // 2, 1, 2
        Tuple t2 = new Tuple(td);
        t2.setField(0, new IntField(2));
        t2.setField(1, new IntField(1));
        t2.setField(2, new IntField(2));
        try {
            Database.getBufferPool().insertTuple(tid, table1.getId(), t1);
            Database.getBufferPool().insertTuple(tid, table1.getId(), t2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 3, 1, 3
        Tuple t3 = new Tuple(td);
        t3.setField(0, new IntField(3));
        t3.setField(1, new IntField(1));
        t3.setField(2, new IntField(3));
        // 4, 1, 4
        Tuple t4 = new Tuple(td);
        t4.setField(0, new IntField(4));
        t4.setField(1, new IntField(1));
        t4.setField(2, new IntField(4));
        try {
            Database.getBufferPool().insertTuple(tid, table2.getId(), t3);
            Database.getBufferPool().insertTuple(tid, table2.getId(), t4);
        } catch (Exception e) {
            e.printStackTrace();
        }

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition

        // some_data_file1.id > 1
        Filter sf1 = new Filter(
                new Predicate(0,
                        Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        // some_data_file1.field1 = some_data_file2.field1
        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}