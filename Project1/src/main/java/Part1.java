import org.omg.CORBA.TIMEOUT;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class Part1 {

    public static void threadRunnableExample0() {
        Runnable task = () -> {
            String threadName = Thread.currentThread().getName();
            System.out.println("Thread: " + threadName);
        };

        task.run();

        Thread thread = new Thread(task);
        thread.start();

        System.out.println("Done!");
    }

    public static void threadRunnableExample1() {
        Runnable runnable = () -> {
            try {
                String name = Thread.currentThread().getName();
                System.out.println("Foo " + name);
                TimeUnit.SECONDS.sleep(1);
                System.out.println("Bar " + name);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public static void executorExample2() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
                String threadName = Thread.currentThread().getName();
                System.out.println("Hello " + threadName);
                // executor services need to be stopped explicitly
        });
    }

    public static void executorExplicitShutdownExample3() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        String threadName = Thread.currentThread().getName();
        System.out.println("Hello " + threadName);
        executor.submit(() -> {
            try {
                System.out.println("Attempt to shutdown executor");
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                System.err.println("Tasks Interrupted");
            } finally {
                if (!executor.isTerminated()) {
                    System.err.println("Cancel non-finished tasks");
                }
                executor.shutdownNow();
                System.out.println("Shutdown finished");
            }
        });
    }

    public static Callable<Integer> callablesFuturesExample4() {
        Callable<Integer> task = () -> {
            try {
                TimeUnit.SECONDS.sleep(1);
                return 123;
            } catch (InterruptedException e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };

        // callable doesn't actually return the value
        // need future for that
        return task;
    }

    public static void callablesFuturesReturnResultExample5() {
        Callable<Integer> task = callablesFuturesExample4();

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<Integer> future = executor.submit(task);

        try {
            System.out.println("future done? " + future.isDone());

            Integer result = future.get();

            System.out.println("future done? " + future.isDone());
            System.out.println("result: " + result);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
        }

    }

    public static void callablesFuturesTimeoutsExample6() {
        ExecutorService executor = Executors.newFixedThreadPool(1);

        Future<Integer> future = executor.submit(() -> {
           try {
               TimeUnit.SECONDS.sleep(2);
               return 123;
           } catch (InterruptedException e) {
               throw new IllegalStateException("task interrupted", e);
           }
        });

        try {
            Integer result = future.get(1, TimeUnit.SECONDS);
            System.out.println(result);
        } catch (InterruptedException | TimeoutException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
        }

        // I get that this is supposed to throw an error, but why?
    }

    public static void invokeAllExample7() {
        // why do we not need to explicitly shutdown this executor?
        ExecutorService executor = Executors.newWorkStealingPool();

        List<Callable<String>> callables = Arrays.asList(
                () -> "task 1",
                () -> "task 2",
                () -> "task 3"
        );

        try {
            executor.invokeAll(callables)
                    .stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    })
                    .forEach(System.out::println);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void invokeAnyExample8() {
        // perhaps the Executors.newWorkStealingPool shuts down automatically?
        ExecutorService executor = Executors.newWorkStealingPool();

        List<Callable<String>> callables = Arrays.asList(
                callable("task1", 2),
                callable("task2", 1),
                callable("task3", 3));

        try {
            String result = executor.invokeAny(callables);
            System.out.println(result);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    // Example 8 helper
    public static Callable<String> callable(String result, long sleepSeconds) {
        return () -> {
            TimeUnit.SECONDS.sleep(sleepSeconds);
            return result;
        };
    };

    // Example 9 Scheduled Executors
    public static void scheduledExecutorsExample9() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        Runnable task = () -> System.out.println("Scheduling: " + System.nanoTime());
        ScheduledFuture<?> future = executor.schedule(task, 3, TimeUnit.SECONDS);

        try {
            TimeUnit.MILLISECONDS.sleep(1337);

            long remainingDelay = future.getDelay(TimeUnit.MILLISECONDS);
            System.out.printf("Remaining Delay: %sms", remainingDelay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
        }
    }

    public static void scheduledExecutorsFixedRateExample10() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        Runnable task = () -> System.out.println("Scheduling: " + System.nanoTime());

        int initialDelay = 0;
        int period = 1;
        executor.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.SECONDS);
    }

    public static void scheduledExecutorFixedDelayExample11() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        Runnable task = () -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                System.out.println("Scheduling: " + System.nanoTime());
            }
            catch (InterruptedException e) {
                System.err.println("task interrupted");
            }
        };

        executor.scheduleWithFixedDelay(task, 0, 1, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        /* Example0 Threads and Runnables */
        //threadRunnableExample0();

        /* Example1 Threads and Runnables with timeouts */
        //threadRunnableExample1();

        /* Example2 Executors */
        //executorExample2();

        /* Example3 Executors Explicity Shutdown */
        //executorExplicitShutdownExample3();

        /* Example4 Callables and Futures */
        //callablesFuturesExample4();

        /* Example5 Callables and Futures Return Result */
        //callablesFuturesReturnResultExample5();

        /* Example6 Callables and Futures timeouts */
        //callablesFuturesTimeoutsExample6();

        /* Example7 InvokeAll */
        //invokeAllExample7();

        /* Example8 InvokeAny */
        //invokeAnyExample8();

        /* Example9 */
        //scheduledExecutorsExample9();

        /* Example10 */
        //scheduledExecutorsFixedRateExample10();

        /* Example 11 */
        //scheduledExecutorFixedDelayExample11();
    }
}
