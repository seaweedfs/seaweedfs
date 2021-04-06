package org.apache.hadoop.seaweed.ftp.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.Md5PasswordEncryptor;
import org.apache.ftpserver.usermanager.UserFactory;
import org.apache.hadoop.seaweed.ftp.controller.vo.FtpUser;
import org.apache.hadoop.seaweed.ftp.controller.vo.Result;
import org.apache.hadoop.seaweed.ftp.users.HdfsUserManager;
import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.*;

import java.io.File;

@RestController
@RequestMapping("/user")
@Api(tags = "FTP用户管理")
public class UserController {

    private static Logger log = Logger.getLogger(UserController.class);

    /***
     * {
     *     "name": "test",
     *     "password": "test",
     *     "homeDirectory": "/buckets/test/"
     * }
     * @param ftpUser
     * @return
     */
    @PostMapping("/add")
    @ApiOperation("新增/编辑用户")
    public Result add(@RequestBody FtpUser ftpUser) {
        try {
            HdfsUserManager userManagerFactory = new HdfsUserManager();
            userManagerFactory.setFile(new File(System.getProperty("user.dir") + File.separator + "users.properties"));
            userManagerFactory.setPasswordEncryptor(new Md5PasswordEncryptor());

            UserFactory userFactory = new UserFactory();
            userFactory.setHomeDirectory(ftpUser.getHomeDirectory());
            userFactory.setName(ftpUser.getName());
            userFactory.setPassword(ftpUser.getPassword());
            userFactory.setEnabled(ftpUser.isEnabled());
            userFactory.setMaxIdleTime(ftpUser.getMaxIdleTime());

            User user = userFactory.createUser();
            userManagerFactory.save(user, ftpUser.isRenamePush());
            return new Result(true, "新建用户成功");
        }catch (Exception e) {
            log.error(e);
            return new Result(false, "新建用户失败");
        }
    }

    @DeleteMapping("/delete/{user}")
    @ApiOperation("删除用户")
    public Result delete(@PathVariable(value = "user") String user) {
        try {
            HdfsUserManager userManagerFactory = new HdfsUserManager();
            userManagerFactory.setFile(new File(System.getProperty("user.dir") + File.separator + "users.properties"));
            userManagerFactory.delete(user);
            return new Result(true, "删除用户成功");
        }catch (Exception e) {
            log.error(e);
            return new Result(false, "删除用户失败");
        }
    }

    @GetMapping("/show/{userName}")
    @ApiOperation("查看用户")
    public Result show(@PathVariable(value = "userName") String userName) {
        try {
            HdfsUserManager userManagerFactory = new HdfsUserManager();
            userManagerFactory.setFile(new File(System.getProperty("user.dir") + File.separator + "users.properties"));
            User user = userManagerFactory.getUserByName(userName);
            FtpUser ftpUser = new FtpUser(user.getHomeDirectory(), user.getPassword(), user.getEnabled(), user.getName(), user.getMaxIdleTime(), HdfsUserManager.getUserRenamePush(userName));
            return new Result(true, ftpUser, "获取用户信息成功");
        }catch (Exception e) {
            log.error(e);
            return new Result(false, "获取用户信息失败");
        }
    }

    @GetMapping("/list")
    @ApiOperation("列举用户")
    public Result list() {
        try {
            HdfsUserManager userManagerFactory = new HdfsUserManager();
            userManagerFactory.setFile(new File(System.getProperty("user.dir") + File.separator + "users.properties"));
            String[] allUserNames = userManagerFactory.getAllUserNames();
            return new Result(true, allUserNames, "列举用户成功");
        }catch (Exception e) {
            log.error(e);
            return new Result(false, "列举用户失败");
        }
    }
}
