package mydemo.demo;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.*;
import java.time.LocalDateTime;

// documentation used: https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-create-index.html

@RestController
public class MyRestController {
    
    @PostMapping("/createindex")
    public ResponseEntity<String> CreateIndex(@RequestParam String index, @RequestHeader (name = "Authorization") String authHeader) {

        if (!validRequestHeaders(index,"0")) {
            ResponseEntity.status(HttpStatus.BAD_REQUEST);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("The index name provided contains illegal characters (\\ , # {} :)");
        }

        RestClientBuilder builder = createRestBuilder(authHeader);

        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
                
            CreateIndexRequest request = new CreateIndexRequest(index);
            request.settings(Settings.builder() 
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1));

            request.mapping( 
                "{\n" +
                    "  \"properties\": {\n" +
                    "    \"id\": { \"type\": \"integer\" },\n" +
                    "    \"doc\": { \"type\": \"text\" },\n" +
                    "    \"postDate\": { \"type\": \"text\" }\n" +
                    "  }\n" +
                    "}",
                    XContentType.JSON
            );

            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
                
            if (createIndexResponse.isAcknowledged() & createIndexResponse.isShardsAcknowledged()) {
                return ResponseEntity.status(HttpStatus.OK).body("Index " + index + " was created!");
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("There was a problem creating the index " + index);
            }

        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred creating the index: " + handleException(e));
        }
    }

    @PostMapping("/insertdoc")
    public ResponseEntity<String> insertDoc(@RequestParam String index, @RequestParam String id, @RequestHeader (name = "Authorization") String authHeader) {

        if (!validRequestHeaders(index, id)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("The index or ID provided contains illegal characters or is not a valid ID (\\ , # {} :)");
        }

        RestClientBuilder builder = createRestBuilder(authHeader);

        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {

            IndexRequest request = new IndexRequest(index);
              request.id(id);

            String jsonString = "{" +
            "\"doc\":\"fcb-" + id + "\"," +
            "\"id\":\"" + id + "\", " +
            "\"postDate\":\"" + LocalDateTime.now() + "\"" +
            "}";

            request.source(jsonString, XContentType.JSON); 

            request.timeout(TimeValue.timeValueSeconds(10)); 
            request.timeout("10s"); 

            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED){
                return ResponseEntity.status(HttpStatus.OK).body("Document was successfuly created" + "\n" + indexResponse.toString());
            } else if(indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                return ResponseEntity.status(HttpStatus.OK).body("Document was successfuly updated " + "\n" + indexResponse.toString());
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("There was a problem inserting the document " + indexResponse.toString());
            }

        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("An unexpected error occurred creating the doc: " + handleException(e));
        }
    }

    @GetMapping("/getdoc")
    public ResponseEntity<String> getDoc(@RequestParam String index, @RequestParam String id, @RequestHeader (name = "Authorization") String authHeader) {
        
        if (!validRequestHeaders(index, id)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("The index or ID provided contains illegal characters or is not a valid ID (\\ , # {} :)");
        }

        RestClientBuilder builder = createRestBuilder(authHeader);

        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {

            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            sourceBuilder.query(QueryBuilders.idsQuery().addIds(id));
            searchRequest.source(sourceBuilder);
            
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            if (searchResponse.getHits().getTotalHits().value > 0){
                return ResponseEntity.status(HttpStatus.OK).body(searchResponse.toString());
            } else {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("No documents found for the provided ID. Try a different ID.");
            }


        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(handleException(e));
        }   
    }



    private RestClientBuilder createRestBuilder(String authHeader){

        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200)) // elasticsearch api uses port 9200
                .setDefaultHeaders(new Header[]{new BasicHeader(HttpHeaders.AUTHORIZATION, authHeader), 
                    new BasicHeader(HttpHeaders.ACCEPT, "application/vnd.elasticsearch+json;compatible-with=7"), // compatibility headers for springboot and elasticsearch api v8+
                    new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/vnd.elasticsearch+json;compatible-with=7")}); // source: https://stackoverflow.com/questions/48842352/elasticsearch-java-resthighlevelclient-unable-to-parse-response-body-illegalar

        return builder;
    }

    private String handleException(IOException e) {

        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter( writer );
        e.printStackTrace( printWriter );
        printWriter.flush();

        return writer.toString();
    }

    private boolean validRequestHeaders (String index, String id) {

        boolean idIsInteger = false;
        boolean indexIsValid = false;

        try{

            int convertedId = Integer.parseInt(id);
            idIsInteger = true;

        } catch (Exception e) {
            return false;
        }

        try {
            final Pattern pattern = Pattern.compile("\\[.*\\].*#.*'.*'.*\\{.*\\},.*:"); // check if \ , # {} : are used in the index name 
            final Matcher matcher = pattern.matcher(index);
            indexIsValid = !matcher.matches();
        } catch (Exception e) {
            return false;
        }
        
        return idIsInteger & indexIsValid;

    }
}