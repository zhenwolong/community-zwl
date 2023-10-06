package com.zwl.communityzwl.dao.elasticsearch;


import com.zwl.communityzwl.entity.DiscussPost;
import com.zwl.communityzwl.entity.DiscussPost;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DiscussPostRepository extends ElasticsearchRepository<DiscussPost, Integer> {

}
