package com.zwl.communityzwl.controller;

import com.alibaba.fastjson.JSONObject;
import com.zwl.communityzwl.entity.Comment;
import com.zwl.communityzwl.entity.DiscussPost;
import com.zwl.communityzwl.entity.Event;
import com.zwl.communityzwl.event.EventConsumer;
import com.zwl.communityzwl.event.EventProducer;
import com.zwl.communityzwl.service.CommentService;
import com.zwl.communityzwl.service.DiscussPostService;
import com.zwl.communityzwl.service.ElasticsearchService;
import com.zwl.communityzwl.utils.CommunityConstant;
import com.zwl.communityzwl.utils.HostHolder;
import com.zwl.communityzwl.utils.RedisKeyUtil;
import com.zwl.communityzwl.event.EventConsumer;
import com.zwl.communityzwl.event.EventProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.Date;


@Controller
@RequestMapping("/comment")
public class CommentController implements com.zwl.communityzwl.utils.CommunityConstant {

    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);
    @Autowired
    private com.zwl.communityzwl.service.CommentService commentService;

    @Autowired
    private com.zwl.communityzwl.utils.HostHolder hostHolder;

    @Autowired
    private EventProducer eventProducer;

    @Autowired
    private com.zwl.communityzwl.service.DiscussPostService discussPostService;

    @Autowired
    private com.zwl.communityzwl.service.ElasticsearchService elasticsearchService;

    @Autowired
    private RedisTemplate redisTemplate;


    @RequestMapping(path = "/add/{discussPostId}", method = RequestMethod.POST)
    public String addComment(@PathVariable("discussPostId") int discussPostId, Comment comment) {
        comment.setUserId(hostHolder.getUser().getId());
        comment.setStatus(0);
        comment.setCreateTime(new Date());
        commentService.addComment(comment);

        //触发评论事件
        Event event = new Event()
                .setTopic(TOPIC_COMMENT)
                .setUserId(hostHolder.getUser().getId())
                .setEntityType(comment.getEntityType())
                .setEntityId(comment.getEntityId())
                .setData("postId", discussPostId);
        if (comment.getEntityType() == ENTITY_TYPE_POST) {
            DiscussPost target = discussPostService.findDiscussPostById(comment.getEntityId());
            event.setEntityUserId(target.getUserId());
        } else if (comment.getEntityType() == ENTITY_TYPE_COMMENT) {
            Comment target = commentService.findCommentById(comment.getEntityId());
            event.setEntityUserId(target.getUserId());
        }
        eventProducer.fireEvent(event);

        if (comment.getEntityType() == ENTITY_TYPE_POST)
            //触发发帖事件
            event = new Event()
                    .setTopic(TOPIC_PUBLISH)
                    .setUserId(comment.getUserId())
                    .setEntityType(ENTITY_TYPE_POST)
                    .setEntityId(discussPostId);
        eventProducer.fireEvent(event);

        //计算帖子分数
        String redisKey = com.zwl.communityzwl.utils.RedisKeyUtil.getPostScoreKey();
        redisTemplate.opsForSet().add(redisKey,discussPostId);

        return "redirect:/discuss/detail/" + discussPostId;
    }

    // 消费发帖事件
    @KafkaListener(topics = {TOPIC_PUBLISH})
    public void handlePublishMessage(ConsumerRecord record) {
        if (record == null || record.value() == null) {
            logger.error("消息的内容为空!");
            return;
        }

        Event event = JSONObject.parseObject(record.value().toString(), Event.class);
        if (event == null) {
            logger.error("消息格式错误!");
            return;
        }

        com.zwl.communityzwl.entity.DiscussPost post = discussPostService.findDiscussPostById(event.getEntityId());
        elasticsearchService.saveDiscussPost(post);
    }

}
