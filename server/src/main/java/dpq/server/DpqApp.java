package dpq.server;

import dpq.core.QueueService;
import io.javalin.Javalin;

public final class DpqApp {

    private DpqApp() {}

    public static Javalin create(QueueService service) {
        return Javalin.create();
    }
}
