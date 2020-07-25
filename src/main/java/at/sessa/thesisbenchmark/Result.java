package at.sessa.thesisbenchmark;

import java.util.HashMap;

public class Result {
    private String databaseType;
    private HashMap<Integer, Long> queryExecutionTimes = new HashMap<>();

    public Result(String databaseType) {
        this.databaseType = databaseType;
    }

    public void addQueryExecutionTime(int queryNumber, long timeTaken) {
        queryExecutionTimes.put(queryNumber, timeTaken);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(databaseType);
        stringBuilder.append("\n");

        queryExecutionTimes.forEach((k,v) -> {
            stringBuilder.append(k);
            stringBuilder.append(",");
            stringBuilder.append(v);
            stringBuilder.append("\n");
        });

        return stringBuilder.toString();
    }
}
