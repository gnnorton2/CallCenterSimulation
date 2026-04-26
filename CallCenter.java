
//Brielle Norton & Alexis Evans
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
    /*
     * Total number of customers that each agent will serve in this simulation.
     */
    private static final int CUSTOMERS_PER_AGENT = 5;
    /*
     * Total number of agents.
     */
    private static final int NUMBER_OF_AGENTS = 3;
    /*
     * Total number of customers to create for this simulation.
     */
    private static final int NUMBER_OF_CUSTOMERS = NUMBER_OF_AGENTS *
            CUSTOMERS_PER_AGENT;
    /*
     * Number of threads to use for this simulation.
     */
    private static final int NUMBER_OF_THREADS = 10;
    private static final Queue<Integer> GREETING_QUEUE = new LinkedList<>();
    private static final Queue<Integer> CALL_QUEUE = new LinkedList<>();
    private static final Lock lock = new ReentrantLock();
    private static final Condition notEmptyGreeting = lock.newCondition();
    private static final Condition notEmptyCall = lock.newCondition();

    /*
     * The Agent class.
     */
    public static class Agent implements Runnable {
        /*
         * Tracks assigned random IDs so each agent still gets a unique value.
         */
        private static final Set<Integer> USED_AGENT_IDS = new HashSet<>();
        // The ID of the agent
        private final int ID;

        public Agent() {
            ID = generateUniqueAgentID();
        }

        // Kept for compatibility with existing code that may still pass an int.
        public Agent(int i) {
            this();
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
            for (int customersServed = 0; customersServed < CUSTOMERS_PER_AGENT; customersServed++) {
                try {
                    lock.lock();
                    int customerID;
                    try {
                        while (CALL_QUEUE.isEmpty()) {
                            notEmptyCall.await();
                        }
                        customerID = CALL_QUEUE.remove();
                    } finally {
                        lock.unlock();
                    }
                    serve(customerID);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /*
         * Your implementation must call the method below to serve each customer.
         * Do not modify this method.
         */
        public void serve(int customerID) {
            System.out.println("Agent " + ID + " is serving customer " +
                    customerID);
            try {
                // Simulate busy serving a customer by sleeping for a random
                // period.
                sleep(ThreadLocalRandom.current().nextInt(10, 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * The greeter class.
     */
    public static class Greeter implements Runnable {
        public void run() {
            int greetedCustomers = 0;
            while (greetedCustomers < NUMBER_OF_CUSTOMERS) {
                try {
                    lock.lock();
                    try {
                        while (GREETING_QUEUE.isEmpty()) {
                            notEmptyGreeting.await();
                        }
                        int customerID = GREETING_QUEUE.remove();
                        greet(customerID);
                        CALL_QUEUE.add(customerID);
                        notEmptyCall.signalAll();
                        System.out.println("Customer: " + customerID + " has been placed in the serve queue.");
                        greetedCustomers++;
                    } finally {
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        /*
         * Your implementation must call the method below to greet each customer.
         * Do not modify this method.
         */
        public void greet(int customerID) {
            System.out.println("Greeting customer " + customerID);
            try {
                // Simulate busy greeting a customer by sleeping for a random period.
                sleep(ThreadLocalRandom.current().nextInt(10, 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * The customer class.
     */
    public static class Customer implements Runnable {
        private final int ID;

        public Customer(int i) {
            ID = i;
        }

        public void run() {
            lock.lock();
            System.out.println("Customer " + ID + " has arrived.");
            try {
                GREETING_QUEUE.add(ID);
                notEmptyGreeting.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    /*
     * Create the greeter and agents tasks first, and then create the customer
     * tasks.
     * to simulate a random interval between customer calls, sleep for a random
     * period after creating each customer task.
     */
    public static void main(String[] args) {
        Thread greeterThread = new Thread(new Greeter());
        greeterThread.start();
        for (int i = 1; i <= 5; i++) {
            new Thread(new Customer(i)).start();
        }
    }
}
