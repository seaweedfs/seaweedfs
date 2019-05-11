Mount the SeaweedFS via FUSE

# Mount by fstab


```
$ # on linux
$ sudo apt-get install fuse
$ sudo echo 'user_allow_other' >> /etc/fuse.conf
$ sudo mv weedfuse /sbin/mount.weedfuse

$ # on Mac
$ sudo mv weedfuse /sbin/mount_weedfuse

```

On both OS X and Linux, you can add one of the entries to your /etc/fstab file like the following:

```
# mount the whole SeaweedFS
localhost:8888/    /home/some/mount/folder  weedfuse

# mount the SeaweedFS sub folder
localhost:8888/sub/dir    /home/some/mount/folder  weedfuse

# mount the SeaweedFS sub folder with some options
localhost:8888/sub/dir    /home/some/mount/folder  weedfuse  user

```

To verify it can work, try this command
```
$ sudo mount -av

...

/home/some/mount/folder           : successfully mounted

```

If you see `successfully mounted`, try to access the mounted folder and verify everything works.


To debug, run these:
```

$ weedfuse -foreground localhost:8888/ /home/some/mount/folder

```


To unmount the folder:
```

$ sudo umount /home/some/mount/folder

```

<!-- not working yet!

# Mount by autofs

AutoFS can mount a folder if accessed.

```
# install autofs
$ sudo apt-get install autofs
```

Here is an example on how to mount a folder for all users under `/home` directory.
Assuming there exists corresponding folders under `/home` on both local and SeaweedFS.

Edit `/etc/auto.master` and `/etc/auto.weedfuse` file with these content
```
$ cat /etc/auto.master
/home    /etc/auto.weedfuse

$ cat /etc/auto.weedfuse
# map /home/<user> to localhost:8888/home/<user>
* -fstype=weedfuse,rw,allow_other,foreground :localhost\:8888/home/&

```

-->
