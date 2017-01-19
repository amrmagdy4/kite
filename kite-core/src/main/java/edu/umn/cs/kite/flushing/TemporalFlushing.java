package edu.umn.cs.kite.flushing;

/**
 * Created by amr_000 on 8/6/2016.
 */
public class TemporalFlushing implements FlushingPolicy {
    private int period;

    public enum PeriodType {TIME, DATA_SIZE};
    private PeriodType periodType;

    public TemporalFlushing(int period, PeriodType type) {
        this.period = period;
        this.periodType = type;
    }

    public int getPeriod() {
        return period;
    }

    public PeriodType getPeriodType() {
        return periodType;
    }

    @Override
    public FlushingPolicyType getPolicyType() {
        return FlushingPolicyType.TEMPORAL_FLUSHING;
    }
}
