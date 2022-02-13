package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Field[] fields;
    private TupleDesc td;
    private RecordId rid = null;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        int numFields = td.numFields();
        fields = new Field[numFields];
        Iterator<TupleDesc.TDItem> it = td.iterator();
        for (int i = 0; i < numFields; i++) {
            TupleDesc.TDItem item = it.next();
            switch (item.fieldType) {
                case INT_TYPE:
                    fields[i] = new IntField(0);
                    break;
                case STRING_TYPE:
                    fields[i] = new StringField("", Type.STRING_LEN);
                    break;
                default:
                    System.out.println("unknown fieldType");
                    break;
            }
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        fields[i] = f;
    }

    public void fromJoinTwoTuples(Tuple t1, Tuple t2) {
        assert (this.getTupleDesc().numFields() == t1.getTupleDesc().numFields() + t2.getTupleDesc().numFields());
        int i = 0;
        for (int j = 0; j < t1.getTupleDesc().numFields(); j++) {
            setField(i++, t1.getField(j));
        }
        for (int j = 0; j < t2.getTupleDesc().numFields(); j++) {
            setField(i++, t2.getField(j));
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder res = new StringBuilder(128);
        for (Field field: fields) {
            res.append(field.toString()).append(" ");
        }
        return res.toString();
//        throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return Arrays.stream(fields).iterator();
//        return null;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.td = td;
    }
}
