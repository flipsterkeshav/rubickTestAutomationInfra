package tests.engage.engage_api.rubick.DAO;

import java.util.Map;

/**
 * Created by keshav.gupta on 21/05/15.
 */
public class Action {
    private Map<String, String> attributeMap;
    private String actionId;
    private long timestamp;

    public Action(Map<String, String> attributeMap, String actionId, long timestamp) {
        this.setAttributeMap(attributeMap);
        this.setActionId(actionId);
        this.setTimestamp(timestamp);
    }




    public String getAttributeValue( String key ) {
        return getAttributeMap().get(key);
    }

    public void setAttribute( String key, String value ) {
        getAttributeMap().put(key, value);
    }

    public String getActionId() {
        return actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getAttributeMap() {
        return attributeMap;
    }

    public void setAttributeMap(Map<String, String> attributeMap) {
        this.attributeMap = attributeMap;
    }
}
