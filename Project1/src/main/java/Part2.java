import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class Part2 {

    private int count;

    private void increment() {
        count += 1;
    }

    private synchronized void incrementSync() {
        count += 1;
    }

    private static void stop(ExecutorService executor) {
        executor.shutdownNow();
    }

    private void synchronizedExample0() {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        IntStream.range(0, 10000)
                .forEach(i -> executor.submit(this::increment));

        stop(executor);

        System.out.println(count);
        // get a different value every time because the 2 threads are sharing a variable
        // but they fail to synchronize
    }

    private void synchronizedExample1() {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        IntStream.range(0, 10000)
                .forEach(i -> executor.submit(this::incrementSync));

        stop(executor);

        System.out.println(count);
        // this correctly prints out 10000 because Java supports thread synchronization
    }

    private void reentrantLockExample2() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ReentrantLock lock = new ReentrantLock();

        executor.submit(() -> {
            lock.lock();
            try {
                count += 1;
            } finally {
                lock.unlock();
            }
        });

        executor.submit(() -> {
           System.out.println("Locked: " + lock.isLocked());
           System.out.println("Held by me: " + lock.isHeldByCurrentThread());
           boolean locked = lock.tryLock();
           System.out.println("Lock acquired: " + locked);
        });

        stop(executor);
    }

    private void readWriteLockExample3() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Map<String, String> map = new HashMap<>();
        ReadWriteLock lock = new ReentrantReadWriteLock();

        executor.submit(() -> {
            lock.writeLock().lock();
            try {
                sleep(1);
                map.put("foo", "bar");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.writeLock().unlock();
            }
        });

        Runnable readTask = () -> {
            lock.readLock().lock();
            try {
                System.out.println(map.get("foo"));
                sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.readLock().unlock();
            }
        };

        executor.submit(readTask);
        executor.submit(readTask);

        stop(executor);
    }

    private void stampedLocksExample4() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Map<String, String> map = new HashMap<>();
        StampedLock lock = new StampedLock();

        executor.submit(() -> {
            long stamp = lock.writeLock();
            try {
                sleep(1);
                map.put("foo", "bar");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlockWrite(stamp);
            }
        });

        Runnable readTask = () -> {
            long stamp = lock.readLock();
            try {
                System.out.println(map.get("foo"));
                sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlockRead(stamp);
            }
        };

        executor.submit(readTask);
        executor.submit(readTask);

        stop(executor);
    }

    private void stampedLocksOptimisticLockingExample5() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        StampedLock lock = new StampedLock();

        executor.submit(() -> {
            long stamp = lock.tryOptimisticRead();
            try {
                System.out.println("Optimistic Lock Valid: " + lock.validate(stamp));
                sleep(1);
                System.out.println("Optimistic Lock valid: " + lock.validate(stamp));
                sleep(2);
                System.out.println("Optimistic Lock Valid: " + lock.validate(stamp));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock(stamp);
            }
        });

        executor.submit(() -> {
            long stamp = lock.writeLock();
            try {
                System.out.println("Write Lock Acquired");
                sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock(stamp);
                System.out.println("Write Done");
            }
        });

        stop(executor);

    }

    public void semaphoresExample6() {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        Semaphore semaphore = new Semaphore(5);

        Runnable longRunningTask = () -> {
            boolean permit = false;
            try {
                permit = semaphore.tryAcquire(1, TimeUnit.SECONDS);
                if (permit) {
                    System.out.println("Semaphore acquired");
                    sleep(5);
                } else {
                    System.out.println("Could not acquire semaphore");
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            } finally {
                if (permit) {
                    semaphore.release();
                }
            }
        };

        IntStream.range(0, 10)
                .forEach(i -> executor.submit(longRunningTask));

        stop(executor);
    }

    public static void main(String[] args) {
        Part2 examples = new Part2();

        /* Example0 Unsynched Increment */
        //examples.synchronizedExample0();

        /* Example1 Synchronized Increment */
        //examples.synchronizedExample1();

        /* Example2 Reentrant Locks */
        //examples.reentrantLockExample2();

        /* Example3 ReadWrite Locks */
        //examples.readWriteLockExample3();

        /* Example4 Stamp Locks */
        //examples.stampedLocksExample4();

        /* Example5 StampLock Optimistic Locking */
        //examples.stampedLocksOptimisticLockingExample5();

        /* Example6 Semaphores */
        examples.semaphoresExample6();
    }
}
