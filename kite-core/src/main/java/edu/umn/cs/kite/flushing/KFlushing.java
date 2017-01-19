package edu.umn.cs.kite.flushing;

/**
 * Created by amr_000 on 8/6/2016.
 */
public class KFlushing implements FlushingPolicy {

    private int k;

    public KFlushing(int k) {
        this.k = k;
    }

    public int getK() {
        return k;
    }

    @Override
    public FlushingPolicyType getPolicyType() {
        return FlushingPolicyType.K_FLUSHING;
    }
}
