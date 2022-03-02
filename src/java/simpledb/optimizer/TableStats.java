package simpledb.optimizer;


import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.index.BTreeFile;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;




/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {


    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int tableId;
    private final int ioCostPerPage;

    public static class ZoneMap {

        private int min;
        private int max;
        private boolean init = false;

        public int getMin() {
            return min;
        }

        public int getMax() {
            return max;
        }

        public ZoneMap() {
        }

        public void addValue(int v) {
            if (!init) {
                min = v;
                max = v;
                init = true;
                return;
            }

            if (v < min) {
                min = v;
            }
            if (v > max) {
                max = v;
            }
        }

    }


    private final Object[] histograms;
    private final ZoneMap[] zoneMaps;
    private int numTups = 0;
    private final TupleDesc td;



    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableId
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableId, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

        this.tableId = tableId;
        this.ioCostPerPage = ioCostPerPage;


        td = Database.getCatalog().getTupleDesc(tableId);
        int numFields = td.numFields();
        this.zoneMaps = new ZoneMap[numFields];
        this.histograms = new Object[numFields];

        Transaction transaction = new Transaction();
        SeqScan scan = new SeqScan(transaction.getId(), tableId);

        try {
            scan.open();
            while (scan.hasNext()) {
                Tuple t = scan.next();
                numTups++;
                for (int i = 0; i < numFields; i++) {
                    Field field = t.getField(i);
                    switch (field.getType()) {
                        case INT_TYPE:
                            if (zoneMaps[i] == null) {
                                zoneMaps[i] = new ZoneMap();
                            }
                            zoneMaps[i].addValue(((IntField)field).getValue());
                            break;
                        default:
                            break;
                    }
                }
            }
            scan.rewind();
            while (scan.hasNext()) {
                Tuple t = scan.next();
                for (int i = 0; i < numFields; i++) {
                    Field field = t.getField(i);
                    switch (field.getType()) {
                        case INT_TYPE:
                            if (histograms[i] == null) {
                                histograms[i] = new IntHistogram(NUM_HIST_BINS, zoneMaps[i].getMin(), zoneMaps[i].getMax());
                            }
                            IntHistogram ihis = getIntHistogram(i);
                            ihis.addValue(((IntField)field).getValue());
                            break;
                        case STRING_TYPE:
                            if (histograms[i] == null) {
                                histograms[i] = new StringHistogram(NUM_HIST_BINS);
                            }
                            StringHistogram shis = getStringHistogram(i);
                            shis.addValue(((StringField)field).getValue());
                            break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            transaction.transactionComplete(false);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private IntHistogram getIntHistogram(int i) {
        assert (histograms[i] instanceof IntHistogram);
        return (IntHistogram) histograms[i];
    }

    private StringHistogram getStringHistogram(int i) {
        assert (histograms[i] instanceof StringHistogram);
        return (StringHistogram) histograms[i];
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
//        return 0;
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        if (file instanceof HeapFile) {
            return ioCostPerPage * ((HeapFile)file).numPages();
        }
        if (file instanceof BTreeFile) {
            return ioCostPerPage * ((BTreeFile)file).numPages();
        }
        return 0;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
//        return 0;
        return (int) (numTups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
//        return 1.0;
        switch (td.getFieldType(field)) {
            case INT_TYPE:
                IntHistogram ihis = getIntHistogram(field);
                return ihis.estimateSelectivity(op, ((IntField)constant).getValue());
            case STRING_TYPE:
                StringHistogram shis = getStringHistogram(field);
                return shis.estimateSelectivity(op, ((StringField)constant).getValue());
        }
        return 0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
//        return 0;
        return numTups;
    }

}
