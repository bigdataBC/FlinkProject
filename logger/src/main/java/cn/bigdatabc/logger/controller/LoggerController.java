package cn.bigdatabc.logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author liufengting
 * @date 2021/12/20
 */

//@RestController = @Controller+@ResponseBody
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLog(@RequestParam(value = "param") String jsonStr) {
//        System.out.println(jsonStr);
        // 落盘
        log.info(jsonStr);

        // 生产消息到指定 topic
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "kfk success";
    }

    @RequestMapping("test1")
    public String test1() {
        System.out.println("success");
        return "success1";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam(value = "nn") String name,
                        @RequestParam(defaultValue = "12") int age) {
        System.out.println("name:" + name + ", age：" + age);
        return name + "-->" + age;
    }
}
