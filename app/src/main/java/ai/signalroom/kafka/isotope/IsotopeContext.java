package ai.signalroom.kafka.isotope;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Thread-local handoff for the in-flight isotope between a consumer and a
 * downstream producer in the same processing thread. In a typical
 * consume-then-produce service, the application calls
 * {@link #adoptFromRecord(ConsumerRecord)} before processing each record so
 * that {@link IsotopeProducerInterceptor} can find the inbound trace context
 * and append the next hop instead of starting a new trace.
 */
public final class IsotopeContext {

    private static final ThreadLocal<Isotope> CURRENT = new ThreadLocal<>();

    private IsotopeContext() {}

    public static Isotope current()        { return CURRENT.get(); }
    public static void set(Isotope iso)    { CURRENT.set(iso); }
    public static void clear()             { CURRENT.remove(); }

    /**
     * Extracts the isotope from the given record's headers (if any) and
     * installs it as the current thread-local context. Returns the isotope
     * adopted, or {@code null} if the record carried no isotope.
     */
    public static Isotope adoptFromRecord(ConsumerRecord<?, ?> record) {
        Isotope iso = Isotope.fromHeaders(record.headers());
        if (iso != null) {
            set(iso);
        } else {
            clear();
        }
        return iso;
    }
}
