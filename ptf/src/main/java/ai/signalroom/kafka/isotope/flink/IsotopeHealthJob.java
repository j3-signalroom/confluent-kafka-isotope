package ai.signalroom.kafka.isotope.flink;

import java.time.ZoneId;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Entry point for the opt-in <b>health monitor</b>: a second CMF Application that
 * watches {@link IsotopeReportsJob} from the outside, through the topics it reads and
 * writes (see {@link ReportsHealthPTF} for what counts as unhealthy).
 *
 * <p>It has to be a separate application because Stuck Trace can't report on the job
 * it runs inside: when that job stops, its alerts stop too, and the silence looks
 * like nothing being stuck. This one has its own JobManager and TaskManager, so the
 * reports job failing doesn't take it down. It still shares Kafka, CMF and the Flink
 * operator with that job, and it publishes its alerts to Kafka, so it can't report an
 * outage of any of those.
 */
public final class IsotopeHealthJob {

    private static final List<String> DDL_FILES = List.of("97_reports_health_tables.fql");

    private static final String HEALTH_INSERT_FILE = "98_reports_health.fql";

    private static final long CHECKPOINT_INTERVAL_MS = 30_000L;

    private IsotopeHealthJob() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        // Required, not just a precaution: the report topic going quiet is the very
        // condition being detected, and without this it would hold this job's
        // watermark back, so the alert's timer could never fire.
        tableEnv.getConfig().set("table.exec.source.idle-timeout", IsotopeReportsJob.SOURCE_IDLE_TIMEOUT);

        tableEnv.createTemporarySystemFunction("REPORTS_HEALTH", ReportsHealthPTF.class);

        for (String file : DDL_FILES) {
            for (String stmt : IsotopeReportsJob.statements(IsotopeReportsJob.readResource("sql/" + file))) {
                tableEnv.executeSql(stmt);
            }
        }

        List<String> stmts = IsotopeReportsJob.statements(IsotopeReportsJob.readResource("sql/" + HEALTH_INSERT_FILE));
        tableEnv.executeSql(stmts.get(stmts.size() - 1));
    }
}
