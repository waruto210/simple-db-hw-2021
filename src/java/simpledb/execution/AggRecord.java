package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

public class AggRecord {
    private Aggregator.Op aggType;
    private int currSum = 0;
    private int count = 0;
    private int minMaxValue = 0;
    private int aggField;

    private Tuple resultTuple;
    private boolean isInit = false;

    public AggRecord(Aggregator.Op aggType, int aggField, TupleDesc td) {
        this(aggType ,aggField, td, null);
    }

    public AggRecord(Aggregator.Op aggType, int aggField, TupleDesc td, Field gbFieldValue) {
        this.aggType = aggType;
        this.aggField = aggField;

        if (gbFieldValue == null) {
            this.resultTuple = new Tuple(td);
        } else {
            this.resultTuple = new Tuple(td);
            this.resultTuple.setField(0, gbFieldValue);
        }


    }

    public Tuple getResultTuple() {
        int pos = resultTuple.getTupleDesc().numFields() - 1;
        switch (aggType) {
            case MAX:
            case MIN:
                resultTuple.setField(pos, new IntField(minMaxValue));
                break;
            case SUM:
                resultTuple.setField(pos, new IntField(currSum));
                break;
            case AVG:
                resultTuple.setField(pos, new IntField(currSum / count));
                break;
            case COUNT:
                resultTuple.setField(pos, new IntField(count));
                break;
            default:
                System.out.println("Unsupported Aggregator Type");
        }
        return resultTuple;
    }

    public void init(Tuple tup) {
        switch (aggType) {
            case MAX:
            case MIN:
                minMaxValue = ((IntField)tup.getField(aggField)).getValue();
                break;
            case SUM:
                currSum = ((IntField)tup.getField(aggField)).getValue();
                break;
            case AVG:
                currSum = ((IntField)tup.getField(aggField)).getValue();
            case COUNT:
                count++;
                break;
            default:
                System.out.println("Unsupported Aggregator Type");

        }
        this.isInit = true;
    }

    public void mergeOne(Tuple tup) {
        if (!isInit) {
            init(tup);
            return;
        }

        switch (aggType) {
            case MAX:
                minMaxValue = Math.max(minMaxValue, ((IntField)tup.getField(aggField)).getValue());
                break;
            case MIN:
                minMaxValue = Math.min(minMaxValue, ((IntField)tup.getField(aggField)).getValue());
                break;
            case SUM:
                currSum += ((IntField)tup.getField(aggField)).getValue();
                break;
            case AVG:
                currSum += ((IntField)tup.getField(aggField)).getValue();
            case COUNT:
                count++;
                break;
            default:
                System.out.println("Unsupported Aggregator Type");
        }

    }
}