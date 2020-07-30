package at.sessa.thesisbenchmark;

import java.util.ArrayList;
import java.util.List;

public class Result {
    private final String databaseType;
    private final List<ResultTuple> queryExecutionTimes = new ArrayList<>();

    public Result(String databaseType) {
        this.databaseType = databaseType;
    }

    public void addQueryExecutionTime(ResultTuple resultTuple) {
        queryExecutionTimes.add(resultTuple);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(databaseType);
        stringBuilder.append("\n");

        queryExecutionTimes.forEach(v -> {
            stringBuilder.append(v);
            stringBuilder.append("\n");
        });

        return stringBuilder.toString();
    }

    public enum ResultType {
        HOT,
        COLD;

        @Override
        public String toString() {
            switch (this) {
                case HOT: return "H";
                case COLD: return "C";
                default: throw new IllegalArgumentException();
            }
        }
    }

    public static class ResultTuple {
        private final int queryId;
        private final long timeInMs;
        private final ResultType resultType;

        public ResultTuple(int queryId, long timeInMs, ResultType resultType) {
            this.queryId = queryId;
            this.timeInMs = timeInMs;
            this.resultType = resultType;
        }

        @Override
        public String toString() {
            return queryId+","+timeInMs+","+resultType;
        }
    }
}
