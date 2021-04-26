package org.apache.hadoop.seaweed.ftp.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.seaweed.ftp.service.HFtpService;
import org.apache.hadoop.seaweed.ftp.controller.vo.Result;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/manager")
@Api(tags = "FTP操作管理")
public class FtpManagerController {

    private static Logger log = Logger.getLogger(FtpManagerController.class);

    @Autowired
    private HFtpService hdfsOverFtpServer;

    @GetMapping("/status")
    @ApiOperation("查看FTP服务状态")
    public Result status() {
        Map map = new HashMap<>();
        try {
            boolean status = hdfsOverFtpServer.statusServer();
            map.put("is_running", status);
            return new Result(true, map, "FTP 服务状态获取成功");
        }catch (Exception e) {
            log.error(e);
            map.put("is_running", false);
            return new Result(true, map, "FTP 服务状态获取成功");
        }
    }

    @PutMapping("/start")
    @ApiOperation("启动FTP服务")
    public Result start() {
        try {
            boolean status = hdfsOverFtpServer.statusServer();
            if(!status) {
                hdfsOverFtpServer.startServer();
            }
            return new Result(true, "FTP 服务启动成功");
        }catch (Exception e) {
            log.error(e);
            return new Result(false, "FTP 服务启动失败");
        }
    }

    @PutMapping("/stop")
    @ApiOperation("停止FTP服务")
    public Result stop() {
        try {
            boolean status = hdfsOverFtpServer.statusServer();
            if(status) {
                hdfsOverFtpServer.stopServer();
            }
            return new Result(true, "FTP 服务停止成功");
        }catch (Exception e) {
            log.error(e);
            return new Result(false, "FTP 服务停止失败");
        }
    }
}
