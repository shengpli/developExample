package es.javaapi.example;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ES JAVA API Version is 2.3.4
 * @author lsp
 *
 */
public class ESAPIExample {
    private Client client;
	
	@Before
	public void before(){
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", "myClusterName").build();
		
		try
		{
			client = TransportClient.builder().settings(settings).build()
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300))
			        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));
		} catch (UnknownHostException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@After
	public void after(){
		client.close();
	}
	
	/**
	 * 简单查询
	 * @throws UnknownHostException
	 */
	@Test
	public void simpleQueryTest() throws UnknownHostException{

		
		QueryBuilder qb = QueryBuilders.termQuery(
			    "name",    
			    "kimchy"   
			);
		
		SearchResponse response = client.prepareSearch("index1", "index2")
		        .setTypes("type1", "type2")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
		        .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
		        .setFrom(0).setSize(60).setExplain(true)
		        .addSort(SortBuilders.fieldSort("@timestamp").order(SortOrder.DESC))//sort
		        .execute()
		        .actionGet();
		
		SearchHit[] hits = response.getHits().getHits();
		
		for (SearchHit searchHit : hits)
		{
			//handle searchHit
		}

	}
	
	/**
	 * 滚动查询大量结果
	 */
	@Test
	public void scrollsQueryTest(){
		QueryBuilder qb = QueryBuilders.termQuery("multi", "test");

		SearchResponse scrollResp = client.prepareSearch("test")
		        .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
		        .setScroll(new TimeValue(60000))//查询最大延迟
		        .setQuery(qb)
		        .setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll
		//Scroll until no hits are returned
		while (true) {

		    for (SearchHit hit : scrollResp.getHits().getHits()) {
		        //Handle the hit...
		    }
		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
		    //Break condition: No hits are returned
		    if (scrollResp.getHits().getHits().length == 0) {
		        break;
		    }
	}
	}
	
	/**
	 * Terms Aggregations  字段数量统计
	 */
	
	public void termsAggTest(){
		SearchResponse sr = client.prepareSearch("index1", "index2")
		        .setTypes("type1", "type2")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .addAggregation(AggregationBuilders.terms("genders").field("gender").size(0))//If set to 0, the size will be set to Integer.MAX_VALUE.
		        .execute()
		        .actionGet();
		
		// sr is here your SearchResponse object
		Terms genders = sr.getAggregations().get("genders");

		// For each entry
		for (Terms.Bucket entry : genders.getBuckets()) {
		    entry.getKey();      // Term
		    entry.getDocCount(); // Doc count
		}
	}      
}
