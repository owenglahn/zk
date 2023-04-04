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
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;

// TODO
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
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback, AsyncCallback.DataCallback
{
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isMaster=false;
	boolean initialized =false;
	Optional<WorkerCallback> workerCallback;


	DistProcess(String zkhost)
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

	void initialize()
	{
		try
		{
			runForMaster();	// See if you can become the master (i.e, no other master exists)
			isMaster=true;
			workerCallback = Optional.of(new WorkerCallback());
			getTasks(); // Install monitoring on any new tasks that will be created.
			getWorkers(); // monitor workers
									// TODO monitor for worker tasks?
		}catch(NodeExistsException nee)
		{
			isMaster=false;
			try {
				System.out.println("Calling registerAsWorker()");
				registerAsWorker();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {
				e.printStackTrace();
			}
		} // TODO: What else will you need if this was a worker process?
		catch(UnknownHostException uhe)
		{ System.out.println(uhe); }
		catch(KeeperException ke)
		{ System.out.println(ke); }
		catch(InterruptedException ie)
		{ System.out.println(ie); }

		System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isMaster?"master":"worker"));

	}

	// Master fetching task znodes...
	void getTasks()
	{
		zk.getChildren("/dist02/tasks", this, this, null);
	}

	void getWorkers() {
		zk.getChildren("/dist02/workers", this, workerCallback.get(), null);
	}

	// Try to become the master.
	void runForMaster() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist02/master", pinfo.getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}
	// return list of tasks that are not children of znode /dist02/tasks/
	// i.e. the returned tasks are in the process of completion or have been completed
	List<String> setExistingTasks(List<String> children) throws InterruptedException, KeeperException {
		return children.stream().filter(child -> {
			try {
				List<String> result = zk.getChildren("/dist02/tasks/" + child, false);
				return (result == null || result.isEmpty());
			} catch (KeeperException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}).collect(Collectors.toList());
	}

	void registerAsWorker() throws InterruptedException, KeeperException {
		System.out.println("Registering as worker");
		String workerPath = "/dist02/workers/worker" + ManagementFactory.getRuntimeMXBean().getName();
		zk.create(workerPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		zk.addWatch(workerPath, this, AddWatchMode.PERSISTENT);
	}

	public void process(WatchedEvent e) {
		//Get watcher notifications.

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		System.out.println("DISTAPP : Event received : " + e);

		if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections. {
			// Once we are connected, do our intialization stuff.
			if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initialized == false)
			{
				System.out.println("Initializing...");
				initialize();
				initialized = true;
			}
		}

		// Master should be notified if any new znodes are added to tasks.
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist02/tasks")) {
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getTasks();
		} else if (isMaster && e.getType() == Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist02/workers")) {
			System.out.println("Workers changed");
			getWorkers();
		}

		if (e.getType() ==  Event.EventType.NodeDataChanged && isMaster && e.getPath().contains("/dist02/workers/worker")){
			String workerName = e.getPath().substring("/dist02/workers/".length());
			System.out.println("Worker now available" + workerName);
			// zk.getData(e.getPath(), this, workerCallback.get(), null);
			getWorkers();
		}

		if (e.getType() == Event.EventType.NodeDataChanged && !isMaster && e.getPath().equals("/dist02/workers/worker"
				+ ManagementFactory.getRuntimeMXBean().getName())) {
			new Thread(() -> {
				 try {
					 byte[] taskBytes = zk.getData("/dist02/workers/worker" + pinfo,
							  false, null);
					 String taskName;
					 if (taskBytes != null) {
						 taskName = new String(taskBytes, StandardCharsets.UTF_8);
					 } else {
						 taskName = null;
					 }
					 System.out.println("task name: " + taskName);
					 if (taskName == null) {
						 return;
					 }

					 // get task itself, might not be a good way to do this
					 byte[] taskData = zk.getData(taskName, false, null);


					 // Re-construct our task object.
					 ByteArrayInputStream bis = new ByteArrayInputStream(taskData);
					 ObjectInput in = new ObjectInputStream(bis);
					 DistTask dt = (DistTask) in.readObject();
					 //Execute the task.
					 //TODO: Again, time consuming stuff. Should be done by some other thread and not inside a callback!
					 dt.compute();

					 // Serialize our Task object back to a byte array!
					 ByteArrayOutputStream bos = new ByteArrayOutputStream();
					 ObjectOutputStream oos = new ObjectOutputStream(bos);
					 oos.writeObject(dt);
					 oos.flush();
					 taskData = bos.toByteArray();

					 // Store it inside the result node.
					 zk.create(taskName + "/result", taskData, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					 System.out.println("Setting status as idle");
					 zk.setData("/dist02/workers/worker" + pinfo, null, -1);
				 } catch (KeeperException ex) {
					 try {
						 System.out.println("Setting status as idle");
						 zk.setData("/dist02/workers/worker" + pinfo, null, -1);
					 } catch (KeeperException exc) {
						 throw new RuntimeException(exc);
					 } catch (InterruptedException exc) {
						 throw new RuntimeException(exc);
					 }
				 } catch (InterruptedException ex) {
					 throw new RuntimeException(ex);
				 } catch (IOException ex) {
					 throw new RuntimeException(ex);
				 } catch (ClassNotFoundException ex) {
					 throw new RuntimeException(ex);
				 }
			}).start();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, byte[] taskData, Stat stat) {
		System.out.println("DISTAPP : DATA CALLBACK: processResult : " + rc + ":" + path + ":" + ctx);
		workerCallback.get().issueTask(path.getBytes());
	}

	//Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children) {
		System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
		try {
			children = setExistingTasks(children);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		}
		for(String c: children)
		{
			System.out.println("Waiting task: " + c);
			zk.getData("/dist02/tasks/" + c, false, this, null);
		}
	}

	public static void main(String args[]) throws Exception {
		//Create a new process
		//Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		//Replace this with an approach that will make sure that the process is up and running forever.
//		Thread.sleep(20000);
		while(true) Thread.sleep(1000);
	}

	/**
	 * Used to keep track of new worker nodes
	 */
	private class WorkerCallback implements AsyncCallback.ChildrenCallback, AsyncCallback.DataCallback {
		// blocking queue maybe
		private BlockingQueue<String> availableWorkers;

		WorkerCallback() {
			availableWorkers = new LinkedBlockingQueue<>();
		}

		void issueTask(byte[] taskName) {
			new Thread(() -> {
				try {
					System.out.println("Getting idle worker");
					System.out.println("Idle workers: " + availableWorkers.size());
					String idleWorker = availableWorkers.take();
					System.out.println("Idle worker: " + idleWorker);
					zk.setData("/dist02/workers/" + idleWorker, taskName, -1);
				} catch (InterruptedException e) {
					throw new RuntimeException("Unable to get idle worker");
				} catch (KeeperException e) {
					throw new RuntimeException(e);
				}
			}).start();
		}

		void addIdleWorker(String worker) {
			availableWorkers.add(worker);
		}

		@Override
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			System.out.println("WORKER CALLBACK: processResult : " + rc + ":" + path + ":" + ctx);
			/**
			 * Make concurrent calls to zk to set available workers
			 *
			 * Get new workers from this callback
			 */
			children = children.parallelStream().filter(child ->
			{
				// separate condition like this to prevent unnecessary calls to zk cluster
				if (!availableWorkers.contains(child))
					 try {
						 return zk.getData("/dist02/workers/" + child, DistProcess.this,
								  null) == null;
					 } catch (KeeperException e) {
						 throw new RuntimeException(e);
					 } catch (InterruptedException e) {
						 throw new RuntimeException(e);
					 }
				else return false;
			}).collect(Collectors.toList());
			children.stream().forEach(child -> availableWorkers.add(child));
			System.out.println("availableWorkers length: " + availableWorkers.size());
		}

		@Override
		public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
			String workerName = s.substring("/dist02/workers/".length());
			if (bytes == null) {
				availableWorkers.add(workerName);
			}
		}
	}
}
