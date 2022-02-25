package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private static class Bucket {
        private final int left;
        private final int right;
        private final int width;
        private int count;

        public int getCount() {
            return count;
        }

        public int getLeft() {
            return left;
        }

        public int getRight() {
            return right;
        }

        public void incrementCount() {
            this.count++;
        }



        public Bucket(int left, int right) {
            this.left = left;
            this.right = right;
            this.width = right - left + 1;
            this.count = 0;
        }

        public int getWidth() {
            return width;
        }

        public String toString() {
            return "[" + left + "," + right + "]" + "(" + count + ")";
        }

    }

    private final int numBuckets;
    private final int step;
    private int min;
    private int max;
    private int numTups;
    private final Bucket[] buckets;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "targetBuckets" targetBuckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param targetBuckets The number of targetBuckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int targetBuckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        int range = max - min + 1;
        this.step = (range + targetBuckets - 1 ) / targetBuckets;
        this.numBuckets = (range + step - 1) / step;
        this.buckets = new Bucket[this.numBuckets];
        for (int i = 0; i < this.numBuckets; i++) {
            int left = min + i * step;
            int right = Math.min(left + step - 1, max);
            buckets[i] = new Bucket(left, right);
        }
        this.numTups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        assert (v >= this.min && v <= this.max);
        int index = (v - this.min) / this.step;
        buckets[index].incrementCount();
        this.numTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
//        return -1.0;
        int index = (v - this.min) / this.step;
        double selectivity = 0.0;
        Bucket bucket;
        switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    return 0;
                }
                bucket = buckets[index];
                selectivity = (double) bucket.getCount() / bucket.getWidth() / this.numTups;
                break;
            case NOT_EQUALS:
                if (v < min || v > max) {
                    return 1;
                }
                bucket = buckets[index];
                selectivity = 1.0 - (double) bucket.getCount() / bucket.getWidth() / this.numTups;
                break;
            case GREATER_THAN:
                if (v >= max) {
                    return 0;
                }
                if (v < min) {
                    return 1;
                }
                bucket = buckets[index];
                selectivity = (double) (bucket.getRight() - v) * bucket.getCount() /
                        (this.numTups * bucket.getWidth());
                for (int i = index + 1; i < this.numBuckets; i++) {
                    selectivity += (double) buckets[i].getCount() / this.numTups;
                }
                break;
            case GREATER_THAN_OR_EQ:
                if (v > max) {
                    return 0;
                }
                if (v <= min) {
                    return 1;
                }
                bucket = buckets[index];
                selectivity = (double) (bucket.getRight() - v + 1) * bucket.getCount() /
                        (this.numTups * bucket.getWidth());
                for (int i = index + 1; i < this.numBuckets; i++) {
                    selectivity += (double) buckets[i].getCount() / this.numTups;
                }
                break;
            case LESS_THAN:
                if (v <= min) {
                    return 0;
                }
                if (v > max) {
                    return 1;
                }
                bucket = buckets[index];
                selectivity = (double) (v - bucket.getLeft()) * bucket.getCount() /
                        (this.numTups * bucket.getWidth());
                for(int i = 0; i < index; i++) {
                    selectivity += (double) buckets[i].getCount() / this.numTups;
                }
                break;
            case LESS_THAN_OR_EQ:
                if (v < min) {
                    return 0;
                }
                if (v >= max) {
                    return 1;
                }
                bucket = buckets[index];
                selectivity = (double) (v - bucket.getLeft() + 1) * bucket.getCount() /
                        (this.numTups * bucket.getWidth());
                for(int i = 0; i < index; i++) {
                    selectivity += (double) buckets[i].getCount() / this.numTups;
                }
                break;
            default:
                System.out.println("Invalid operator");

        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
//        return null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.numBuckets; i++) {
            sb.append(buckets[i]);
        }
        return sb.toString();
    }
}
