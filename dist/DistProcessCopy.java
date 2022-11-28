/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva
*/
import java.io.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// Replace 02 with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcessCopy implements Watcher
        , AsyncCallback.ChildrenCallback
{
    ZooKeeper zk;
    String zkServer, pinfo;
    boolean isMaster=false;
    boolean initalized=false;
    ArrayList<String> operatingWorkers;
    ArrayList<String> operatingTasks;
    WorkerCallback workerCallBack;


    DistProcessCopy(String zkhost)
    {
        zkServer=zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
    {
        zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
    }

    void initalize()
    {
        try
        {
            runForMaster();	// See if you can become the master (i.e, no other master exists)
            isMaster=true;
            getTasks(); // Install monitoring on any new tasks that will be created.
            getWorkers(); // Install monitoring on any new workers that will be created.
            workerCallBack = new WorkerCallback();
        }catch(NodeExistsException nee)
        {
            isMaster=false;
            try {
                registerAsWorker();
                runTasks();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
        catch(UnknownHostException uhe)
        { System.out.println(uhe); }
        catch(KeeperException ke)
        { System.out.println(ke); }
        catch(InterruptedException ie)
        { System.out.println(ie); }

        System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isMaster?"master":"worker"));

    }

    private void runTasks() {
        // assign tasks to the zookeeper worker nodes
        // have them hold the file node in their node
        zk.getData("/dist02/workers/worker-"+zkServer, this, workerCallBack, null);
    }

    void getWorkers() {
        zk.getChildren("/dist02/workers", this, this, null);
    }
    // Master fetching task znodes...
    void getTasks()
    {
        zk.getChildren("/dist02/tasks", this, this, null);
    }

    // Try to become the master.
    void runForMaster() throws UnknownHostException, KeeperException, InterruptedException
    {
        //Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
        // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        zk.create("/dist02/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /**
     * register worker when a non master is created
     * registering under /dist02/workers with its zkserver name
     **/
    void registerAsWorker() throws InterruptedException, KeeperException {
        zk.create("/dist02/workers/worker-"+zkServer, "idle".getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void process(WatchedEvent e)
    {
        //Get watcher notifications.

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        System.out.println("DISTAPP : Event received : " + e);

        if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
        {
            // Once we are connected, do our intialization stuff.
            if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initalized == false)
            {
                initalize();
                initalized = true;
            }
        }

        // Master should be notified if any new znodes are added to tasks.
        // Task changes
        if(isMaster && e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist02/tasks"))
        {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the children.
            getTasks();
        }
        // Worker changes
        else if (!isMaster && e.getType() == Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist02/workers")) {
            // There has been changes to the children of the worker node
            // We are going to reinstall all the worker nodes
            getWorkers();
        }
        else if (!isMaster && e.getType() == Event.EventType.NodeDataChanged && e.getPath().equals("/dist02/workers/worker-"+zkServer)){
            // TODO: runTask()
            // this will fix the error in getData for worker
        }
        //
    }

    //Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children) {
        // TODO: ideas:
        // assign task by writing the content of the node into the content of worker node
        // have worker node do the computation

        // Meaning added or deleted
        if (path.equals("/dist02/tasks")) {
            // TODO: if tasks change
            // Assign tasks to idle worker
            // need the list of tasks, if the task hasnt been assigned
            for (String task : children) {
                if (!operatingTasks.contains(task)) {
                    // TODO: assign task to an idle worker
                }
            }
        }
        else if (path.equals("/dist02/workers")) {
            // TODO: if workers change, aka new worker join.
            // meaning new worker is added, since we don't lose workers
            // assign tasks
            for (String worker : children) {
                if (!operatingWorkers.contains(worker)) {
                    // TODO: assign worker to job
                }
            }

        }
    }


    // TODO: fix this
    /**
    //Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children)
    {

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        // This logic is for master !!
        //Every time a new task znode is created by the client, this will be invoked.

        // TODO: Filter out and go over only the newly created task znodes.
        //		Also have a mechanism to assign these tasks to a "Worker" process.
        //		The worker must invoke the "compute" function of the Task send by the client.
        //What to do if you do not have a free worker process?
        System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
        for(String c: children)
        {
            System.out.println(c);
            try
            {
                //TODO There is quite a bit of worker specific activities here,
                // that should be moved done by a process function as the worker.

                //TODO!! This is not a good approach, you should get the data using an async version of the API.
                byte[] taskSerial = zk.getData("/distXX/tasks/"+c, false, null);

                // Re-construct our task object.
                ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
                ObjectInput in = new ObjectInputStream(bis);
                DistTask dt = (DistTask) in.readObject();

                //Execute the task.
                //TODO: Again, time consuming stuff. Should be done by some other thread and not inside a callback!
                dt.compute();

                // Serialize our Task object back to a byte array!
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(dt); oos.flush();
                taskSerial = bos.toByteArray();

                // Store it inside the result node.
                zk.create("/distXX/tasks/"+c+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                //zk.create("/distXX/tasks/"+c+"/result", ("Hello from "+pinfo).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            catch(NodeExistsException nee){System.out.println(nee);}
            catch(KeeperException ke){System.out.println(ke);}
            catch(InterruptedException ie){System.out.println(ie);}
            catch(IOException io){System.out.println(io);}
            catch(ClassNotFoundException cne){System.out.println(cne);}
        }
    }
    **/

    public static void main(String args[]) throws Exception
    {
        //Create a new process
        //Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        //Replace this with an approach that will make sure that the process is up and running forever.
        Thread.sleep(20000);
    }

    private class WorkerCallback implements AsyncCallback.DataCallback {
        @Override

        public void processResult(int i, String path, Object o, byte[] bytes, Stat stat) {
            // idealy we should have the task in the node. or the file name
            // we could have the filename in here and block a worker, since its supposed to be blocking anyways

            try {
                // this gets the file name of the task to process
                byte[] taskFile = zk.getData(path, false, null);
                String filename = new String(bytes, StandardCharsets.UTF_8);

                byte[] taskSerial = zk.getData(filename, false, null);

                ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
                ObjectInput in = new ObjectInputStream(bis);
                DistTask dt = (DistTask) in.readObject();

                dt.compute();

                // Serialize our Task object back to a byte array!
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(dt); oos.flush();
                taskSerial = bos.toByteArray();

                // Store it inside the result node.
                // TODO: this file name is wrong
                zk.create("/dist02/tasks/"+filename+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                //zk.create("/distXX/tasks/"+c+"/result", ("Hello from "+pinfo).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.setData(path, "idle".getBytes(StandardCharsets.UTF_8), 0);

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
