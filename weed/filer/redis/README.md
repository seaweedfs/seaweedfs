Deprecated by redis2.

This implementaiton uses unsorted set. For example, add a directory child via SAdd.

Redis2 moves to sorted set. Adding a child uses ZAddNX.