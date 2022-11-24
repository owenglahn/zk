import org.apache.zookeeper.AsyncCallback;

import java.util.ArrayList;
import java.util.List;

public class WorkerCallback implements AsyncCallback.ChildrenCallback {

    private List<String> availableWorkers;

    public WorkerCallback() {
        availableWorkers = new ArrayList<>();
    }

    public String getIdleWorker() {
        return availableWorkers.isEmpty() ? null:
            availableWorkers.remove(0);
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
        System.out.println("WORKER CALLBACK: processResult : " + rc + ":" + path + ":" + ctx);
        availableWorkers.clear();
        availableWorkers.addAll(children);
    }
}
