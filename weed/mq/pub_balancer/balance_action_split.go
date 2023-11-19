package pub_balancer

/*
Sequence of operations to ensure ordering

Assuming Publisher P10 is publishing to Topic Partition TP10, and Subscriber S10 is subscribing to Topic TP10.
After splitting Topic TP10 into Topic Partition TP11 and Topic Partition TP21,
Publisher P11 is publishing to Topic Partition TP11, and Publisher P21 is publishing to Topic Partition TP21.
Subscriber S12 is subscribing to Topic Partition TP11, and Subscriber S21 is subscribing to Topic Partition TP21.

(The last digit is ephoch generation number, which is increasing when the topic partitioning is changed.)

The diagram is as follows:
P10 -> TP10 -> S10
   ||
   \/
P11 -> TP11 -> S11
P21 -> TP21 -> S21

The following is the sequence of events:
1. Create Topic Partition TP11 and TP21
2. Close Publisher(s) P10
3. Close Subscriber(s) S10
4. Close Topic Partition TP10
5. Start Publisher P11, P21
6. Start Subscriber S11, S21

The dependency is as follows:
          2  => 3  => 4
          |     |
          v     v
    1 => (5  |  6)

And also:
2 => 5
3 => 6

For brokers:
1. Close all publishers for a topic partition
2. Close all subscribers for a topic partition
3. Close the topic partition

*/
