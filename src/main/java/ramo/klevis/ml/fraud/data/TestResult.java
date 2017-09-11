package ramo.klevis.ml.fraud.data;

import java.io.Serializable;

public class TestResult implements Serializable {
    private final long totalFrauds;
    private final long foundFrauds;
    private final long flaggedFrauds;
    private final long missedFrauds;

    public TestResult(long totalFrauds, long foundFrauds, long flaggedFrauds, long missedFrauds) {
        this.totalFrauds = totalFrauds;
        this.foundFrauds = foundFrauds;
        this.flaggedFrauds = flaggedFrauds;
        this.missedFrauds = missedFrauds;
    }

    public long getTotalFrauds() {
        return totalFrauds;
    }

    public long getFoundFrauds() {
        return foundFrauds;
    }

    public long getFlaggedFrauds() {
        return flaggedFrauds;
    }

    public long getMissedFrauds() {
        return missedFrauds;
    }
}