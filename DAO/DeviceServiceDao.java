package tests.engage.engage_api.rubick.DAO;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
//import org.codehaus.jettison.json.JSONArray;
//import org.codehaus.jettison.json.JSONException;
//import org.codehaus.jettison.json.JSONObject;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import tests.engage.engage_api.rubick.common.exceptions.DeviceServiceException;
import tests.utils.Http;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by keshav.gupta on 01/07/15.
 */
public class DeviceServiceDao {
    private String deviceServiceBaseURL;
    private Http http;
    ObjectMapper objectMapper;

    private static Map<String, String> requestHeaders = Maps.newHashMap();
    static {
        requestHeaders.put("Accept", "application/json");
    }

    public DeviceServiceDao(String deviceServiceUrl) {

        deviceServiceBaseURL=deviceServiceUrl;
        http=new Http();
        objectMapper= new ObjectMapper();
    }
    /**
     *
     * This Function will fetch all the information about the devices on which user has logged in.
     *
     * @param userAccountId
     * @return
     * @throws JSONException
     * @throws IOException
     * @throws DeviceServiceException
     */

    public List<Map<String, Object>> fetchDevicesForUser( String userAccountId ) throws JSONException, IOException, DeviceServiceException {
        String allDevicesInfoJsonString = getDeviceInfoForUser(userAccountId);
        JSONArray jsonArray = new JSONArray(allDevicesInfoJsonString);
        List<Map<String, Object>> response = Lists.newArrayList();
        for ( int index = 0; index < jsonArray.length(); index++ ) {
            JSONObject deviceJsonObject = new JSONObject(jsonArray.getString(index));
            if ( deviceJsonObject.getString("appId").equals("retail") ) {
                String deviceInfoString = jsonArray.getString(index);
                //Map<String, Object> deviceInfoMap = RubickHelper.objectMapper.readValue(deviceInfoString, new TypeReference<HashMap<String, Object>>() {});
                Map<String, Object> deviceInfoMap = objectMapper.readValue(deviceInfoString, new TypeReference<HashMap<String, Object>>() {});
                response.add(deviceInfoMap);
            }
        }
        return response;
    }

    /**
     * This funcation will give all the user information given device id.
     * @param deviceId
     * @return
     * @throws IOException
     * @throws DeviceServiceException
     */

    public Map<String, Object> userInfoForDevice(String deviceId) throws IOException, DeviceServiceException {
        //return RubickHelper.objectMapper.readValue(getUserInfoForDevice(deviceId),
        return objectMapper.readValue(getUserInfoForDevice(deviceId),
                new TypeReference<HashMap<String, Object>>() {});
    }

    private String getDeviceInfoForUser( String userAccountID ) throws DeviceServiceException {
        try {
            //return HttpCaller.get(constructURLForDeviceInfo(userAccountID), requestHeaders, "", MediaType.APPLICATION_JSON, String.class);
            return http.get("",constructURLForDeviceInfo(userAccountID));
        } catch ( Exception e ) {
            throw new DeviceServiceException("exception while sending Request to device service.", e);
        }
    }

    private String constructURLForDeviceInfo( String deviceId ) {
        //return (deviceServiceBaseURL.concat("query/accountId/").concat(userId));
        return (deviceServiceBaseURL.concat("devices/"+deviceId));
    }

    private String getUserInfoForDevice(String deviceId) throws DeviceServiceException {
        try {
            //return HttpCaller.get(constructURLForUserInfo(deviceId), requestHeaders, "", MediaType.APPLICATION_JSON, String.class);
            return http.get("",constructURLForUserInfo(deviceId));
        } catch ( Exception e ) {
            throw new DeviceServiceException("exception while sending Request to device service.", e);
        }
    }

    private String constructURLForUserInfo(String deviceId) {
        return (deviceServiceBaseURL.concat(deviceId));
    }

    public String putUserIdDeviceIdEntryInDeviceService(JSONObject jsonObj) throws DeviceServiceException, JSONException {

        String deviceId=jsonObj.getString("deviceId");
        String payload=jsonObj.getJSONObject("payload").toString();
        String url=constructURLForDeviceInfo(deviceId);
        String resultString=http.put("",url,payload);
        return resultString;
    }
    public void deleteDeviceIdInfoInDeviceService(String deviceId) {
        String url=constructURLForDeviceInfo(deviceId);
        http.delete("Delete deviceId info.",url);
    }
}
