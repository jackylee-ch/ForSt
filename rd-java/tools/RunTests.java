import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.io.PrintWriter;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request;

/** Minimal JUnit5 launcher: args = fully-qualified test class names. Exit 1 on any failure. */
public final class RunTests {
    public static void main(String[] args) {
        LauncherDiscoveryRequest req =
                request()
                        .selectors(
                                java.util.Arrays.stream(args)
                                        .map(a -> selectClass(a))
                                        .toArray(org.junit.platform.engine.DiscoverySelector[]::new))
                        .build();
        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.execute(req, listener);
        TestExecutionSummary summary = listener.getSummary();
        PrintWriter out = new PrintWriter(System.out, true);
        summary.printTo(out);
        summary.printFailuresTo(out);
        System.out.printf(
                "TESTS: found=%d started=%d succeeded=%d failed=%d skipped=%d%n",
                summary.getTestsFoundCount(),
                summary.getTestsStartedCount(),
                summary.getTestsSucceededCount(),
                summary.getTestsFailedCount(),
                summary.getTestsSkippedCount());
        System.exit(summary.getTotalFailureCount() > 0 ? 1 : 0);
    }
}
