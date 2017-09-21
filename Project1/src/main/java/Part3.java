import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongBinaryOperator;
import java.util.stream.IntStream;

public class Part3 {

    private static void stop(ExecutorService executor) {
        executor.shutdownNow();
    }

    public void atomicIntExample0() {
        /*
        An operation is atomic when you can safely perform the
        operation in parallel on multiple threads without using the
        `synchronized` keyword or locks.
         */
        AtomicInteger atomicInt = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        IntStream.range(0, 1000)
                .forEach(i -> executor.submit(atomicInt::incrementAndGet));

        stop(executor);

        System.out.println(atomicInt.get());
    }

    public void atomicIntExample1() {
        /*
        AtomicIntegers also support other types of atomic operations
         */
        AtomicInteger atomicInt = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        IntStream.range(0, 1000)
                .forEach(i -> {
                    Runnable task = () ->
                            atomicInt.updateAndGet(n -> n + 2);
                    executor.submit(task);
                });

        stop(executor);

        System.out.println(atomicInt.get());
    }

    public void atomicIntAccumulateExample2() {
        /*
        With accumulate we can perform a SUM of all values from 0 to 1000
         */
        AtomicInteger atomicInt = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        IntStream.range(0, 1000)
                .forEach(i -> {
                    Runnable task = () ->
                            atomicInt.accumulateAndGet(i, (n, m) -> n + m);
                    executor.submit(task);
                });

        stop(executor);

        System.out.println(atomicInt.get());
    }

    public void longAdderExample3() {
        /*
        LongAdder is an alternative to AtomicLong
         */
        LongAdder adder = new LongAdder();

        ExecutorService executor = Executors.newFixedThreadPool(2);

        IntStream.range(0, 1000)
                .forEach(i -> executor.submit(adder::increment));

        stop(executor);

        System.out.println(adder.sumThenReset()); // why does it return 717
    }

    public void longAccumulatorExample4() {
        LongBinaryOperator op = (x, y) -> 2 * x + y;
        LongAccumulator accumulator = new LongAccumulator(op, 1L);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        IntStream.range(0, 1000)
                .forEach(i -> executor.submit(() -> accumulator.accumulate(i)));

        stop(executor);

        System.out.println(accumulator.getThenReset());
    }

    public void concurrentMapExample5() {
        ConcurrentMap<String, String> map = new ConcurrentHashMap<>();
        map.put("foo", "bar");
        map.put("han", "solo");
        map.put("r2", "d2");
        map.put("c3", "p0");

        map.forEach((key, value) -> System.out.printf("%s = %s\n", key, value));

        //String value = map.putIfAbsent("c3", "p1");
        //System.out.println(value); // p0

        //String value = map.getOrDefault("hi", "there");
        //System.out.println(value); // there

        map.replaceAll((key, value) -> "r2".equals(key) ? "d3" : value);
        System.out.println(map.get("r2")); // d3

        //map.comput("foo", (key, value) -> value + value);
        //System.out.println(map.get("foo")); // barbar

        //map.merge("foo", "boo", (oldVal, newVal) -> newVal + " was " + oldVal);
        //System.out.println(map.get("foo"));
    }

    public void concurrentHashMapExample6() {
        System.out.println(ForkJoinPool.getCommonPoolParallelism());

        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        map.put("foo", "bar");
        map.put("han", "solo");
        map.put("r2", "d2");
        map.put("c3", "p0");

        // forEach Example
        map.forEach(1, (key, value) ->
            System.out.printf("key: %s; value: %s; thread: %s\n"
                key, value, Thread.currentThread().getName()));

        // search example
        /*String result = map.search(1, (key, value) -> {
            System.out.println(Thread.currentThread().getName());
            if ("foo".equals(key)) {
                return value;
            }
            return null;
        });
        System.out.println("Result: " + result); */

        // value search example
        /*String resultPrime = map.searchvalues(1, value -> {
            System.out.println(Thread.currentThread().getName());
            if (value.length() > 3) {
                return value;
            }
            return null;
        });
        System.out.println("Result: " + resultPrime); */

        // reduce example
        /*String resultDoublePrime = map.reduce(1,
            (key, value) -> {
                System.out.println("Transform: " + Thread.currentThread().getName());
                return key + "=" + value;
            },
            (s1, s2) -> {
                System.out.println("Reduce: " + Thread.currentThread().getName());
                return s1 + ", " + s2;
            });
        System.out.println("Result Double Prime: " + result); */
    }


    public static void main(String[] args) {
        Part3 examples = new Part3();

        /* Example0 Atomic Integer */
        examples.atomicIntExample0();

        /* Example1 Atomic Integer Operations */
        examples.atomicIntExample1();

        /* Example2 Atomic Integer Accumulate */
        examples.atomicIntAccumulateExample2();

        /* Example3 Long Adder */
        examples.longAdderExample3();

        /* Example4 Long Accumulator */
        examples.longAccumulatorExample4();

        /* Example5 ConcurrentMap */
        examples.concurrentMapExample5();

        /* Example6 ConcurrentHashMap */
        examples.concurrentHashMapExample6();
    }
}
