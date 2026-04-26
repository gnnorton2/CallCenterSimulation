
//Brielle Norton & Alexis Evans
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Queue;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.HashSet;
import java.util.Set;
import static java.lang.Thread.sleep;

public class CallCenter {
    // config
    private static final int CUSTOMERS_PER_AGENT = 5;
    private static final int NUMBER_OF_AGENTS = 3;
    private static final int NUMBER_OF_CUSTOMERS = NUMBER_OF_AGENTS * CUSTOMERS_PER_AGENT;
    private static final int NUMBER_OF_THREADS = 10;
    // shared resources, pipeline queues for call center flow
    private static final Queue<Integer> GREETING_QUEUE = new LinkedList<>();
    private static final Queue<Integer> CALL_QUEUE = new LinkedList<>();
    // single lock protects both shared queues
    private static final Lock lock = new ReentrantLock();
    // used for thread coordination
    private static final Condition notEmptyGreeting = lock.newCondition();
    private static final Condition notEmptyCall = lock.newCondition();

    // agent
    public static class Agent implements Runnable {
        // ensure agent has a unique id
        private static final Set<Integer> USED_AGENT_IDS = new HashSet<>();
        private final int ID;

        public Agent() {
            ID = generateUniqueAgentID();
        }

        private static synchronized int generateUniqueAgentID() {
            int randomID;
            do {
                randomID = ThreadLocalRandom.current().nextInt(1000, 10000);
            } while (USED_AGENT_IDS.contains(randomID));
            USED_AGENT_IDS.add(randomID);
            return randomID;
        }

        public void run() {
            // each agent serves exactly customers_per_agent customers
            for (int i = 0; i < CUSTOMERS_PER_AGENT; i++) {
                int customerID;
                lock.lock();
                try {
                    // wait until a customer is available in service queue
                    while (CALL_QUEUE.isEmpty()) {
                        notEmptyCall.await();
                    }
                    customerID = CALL_QUEUE.remove();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    lock.unlock();
                }
                serve(customerID);
            }
            System.out.println("Agent " + ID + " finished.");
        }

        // simulates servicing a customer
        public void serve(int customerID) {
            System.out.println("Agent " + ID + " is serving customer " + customerID);
            try {
                sleep(ThreadLocalRandom.current().nextInt(10, 1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // greeter
    public static class Greeter implements Runnable {
        public void run() {
            int greeted = 0;
            // process all customers in system
            while (greeted < NUMBER_OF_CUSTOMERS) {
                int customerID;
                lock.lock();
                try {
                    // wait for customers to arrive
                    while (GREETING_QUEUE.isEmpty()) {
                        notEmptyGreeting.await();
                    }
                    // remove customer from arrival queue
                    customerID = GREETING_QUEUE.remove();
                    // greet customer
                    greet(customerID);
                    // move customer to service queue
                    CALL_QUEUE.add(customerID);
                    // notify agents that a customer is available
                    notEmptyCall.signalAll();
                    System.out.println("Customer " + customerID + " placed in serve queue.");
                    greeted++;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    lock.unlock();
                }
            }
        }

        // simulates automated greeting system
        public void greet(int customerID) {
            System.out.println("Greeting customer " + customerID);
            try {
                sleep(ThreadLocalRandom.current().nextInt(10, 1000));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // customer
    public static class Customer implements Runnable {
        private final int ID;

        public Customer(int id) {
            ID = id;
        }

        public void run() {
            System.out.println("Customer " + ID + " has arrived.");
            lock.lock();
            try {
                // add customer to greeting queue
                GREETING_QUEUE.add(ID);
                // notify greeter that a new customer arrived
                notEmptyGreeting.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    // main
    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
        // start greeter thread
        executor.execute(new Greeter());
        // start all agent threads
        for (int i = 0; i < NUMBER_OF_AGENTS; i++) {
            executor.execute(new Agent());
        }
        // create customer arrivals with random delay between them
        for (int i = 1; i <= NUMBER_OF_CUSTOMERS; i++) {
            executor.execute(new Customer(i));
            try {
                sleep(ThreadLocalRandom.current().nextInt(50, 200));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // stop accepting new tasks
        executor.shutdown();
    }
}