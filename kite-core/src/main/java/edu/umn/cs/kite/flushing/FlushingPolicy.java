package edu.umn.cs.kite.flushing;

/**
 * Created by amr_000 on 8/1/2016.
 */
public interface FlushingPolicy {
    public enum  FlushingPolicyType { TEMPORAL_FLUSHING, K_FLUSHING};

    public FlushingPolicyType getPolicyType();

/* Questions that any flushing policy should answer:
        (1) What is the current active index segment?
        (2) When to add a new index segment?
        (3) What data to flush?
        (4) When to flush data???? (this might be always when index memory
                                    budget is full)
*/
}