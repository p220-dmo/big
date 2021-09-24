package sparkstreaming.src.main.java.aga.training.sparkstreaming;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
 Class used to fetch flying data from a Rest API
 */
public class FlightInfoFetcher {
    final static String API_URL = "https://api.schiphol.nl/public-flights/flights" ;
    private String appId ;
    private String appKey ;
    private HttpClient httpClient ;
    private HttpGet preparedHttpRequest ;

    public FlightInfoFetcher(String appId,String apiKey){
        this.appId = appId ;
        this.appKey = apiKey ;
        this.httpClient = HttpClients.createDefault();
        preparedHttpRequest = new HttpGet(API_URL);
        preparedHttpRequest.addHeader("ResourceVersion", "v4");
        preparedHttpRequest.addHeader("app_id", this.appId);
        preparedHttpRequest.addHeader("app_key", this.appKey);
        preparedHttpRequest.addHeader("Accept","application/json");

    }

    public List<String> getFlights(){
        try{
            HttpResponse response = httpClient.execute(this.preparedHttpRequest);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String responseBody = EntityUtils.toString(response.getEntity(), "UTF-8");
                JSONParser parser = new JSONParser();
                JSONObject jsonObject = null;
                try {
                    jsonObject = (JSONObject) parser.parse(responseBody);
                } catch (ParseException e) {
                    return null ;
                }
                JSONArray flights = (JSONArray) jsonObject.get("flights");
                List<String> flightsAsSrring = new ArrayList() ;
                flights.forEach(x -> flightsAsSrring.add(x.toString()));
                return flightsAsSrring ;
            } else {
                return null ;
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null ;
    }
}
