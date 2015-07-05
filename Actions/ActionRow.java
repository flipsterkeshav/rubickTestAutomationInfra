package tests.engage.engage_api.rubick.Actions;

import org.json.JSONException;
import org.json.JSONObject;
import tests.engage.engage_api.rubick.common.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by keshav.gupta on 25/05/15.
 */
public class ActionRow {
    private String url;
    private Map<String,Object> payLoad=new HashMap<String,Object>();
    //private String actionId=Constants._APPLYREFERRALCODE;
    //public Map<String,Object> attributeMap=new HashMap<String,Object>();

    public ActionRow(String url, Map<String, Object> payload) {
        setUrl(url);
        payLoad.put("actionId", Constants._APPLYREFERRALCODE);
        payLoad.put("attributeMap",new HashMap<String,Object>());
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            ((HashMap<String,Object>)payLoad.get("attributeMap")).put(entry.getKey(),entry.getValue());
        }
    }


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public JSONObject fetchJsonObj() {
        JSONObject jsonObj=new JSONObject();
        try {
            jsonObj.put("url",url);
            jsonObj.put("payLoad",payLoad);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObj;
    }
}
