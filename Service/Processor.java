package tests.engage.engage_api.rubick.Service;

import com.google.common.collect.Lists;
import org.json.JSONException;
import org.json.JSONObject;
import tests.engage.engage_api.rubick.DAO.Action;
import tests.engage.engage_api.rubick.DAO.HBaseClient;
import tests.engage.engage_api.rubick.common.Constants;
import tests.engage.engage_api.rubick.common.exceptions.DeviceServiceException;
import tests.engage.engage_api.rubick.common.util.DeviceService;
import tests.utils.Http;
import tests.website.bigfoot.bigfoot_engage.src.UniqueId;

import java.util.*;

/**
 * Created by keshav.gupta on 20/05/15.
 */
public class Processor {

    private static final int ACTION_TYPE_KEY_LENGTH = 4;
    private String rubickCommonUrl="";
    public HBaseClient hbaseClient;
    Http http = new Http();
    private DeviceService deviceService;
    private String deviceServiceUrl;
    //ObjectMapper mapper=new ObjectMapper();


    public Processor() {
        hbaseClient=new HBaseClient();
    }

    public void instantiateDeviceServiceObject() {
        deviceService=new DeviceService(getDeviceServiceUrl());
    }

    public String getRubickCommonUrl() {
        return rubickCommonUrl;
    }

    public void setRubickCommonUrl(String rubickCommonUrl) {
        this.rubickCommonUrl = rubickCommonUrl;
    }

    public String fetchRandomUserId() {
        UniqueId unqid = new UniqueId();
        String userId=unqid.getAccId();
        return userId+"Test";
    }

    public void deleteHBaseRowsForUserId(String table,String userId) {
        try {
            hbaseClient.deleteAllRowsForUserId(table,userId);
        }catch(Exception e) {
            e.printStackTrace();
            logWrapper(e.getMessage());
        }
    }

    public List<Action> fetchDbRowsForUserId(String table,String userId) throws Exception {
        List<Action> actions = Lists.newArrayList();
        Map<String, Map<String, String>> actionHbaseRowData= hbaseClient.getKVMapRowsFromRows(hbaseClient.fetchDbRowsForUserId(table, userId));
        for ( Map.Entry<String, Map<String, String>> entry : actionHbaseRowData.entrySet() ) {
            actions.add(convertRowToAction(entry));
        }
        return actions;
    }

    private Action convertRowToAction( Map.Entry<String, Map<String, String>> hbaseActionRows ) {
        String[] actionData = hbaseActionRows.getKey().split(Constants.HBASE_KEY_DELIMITER);
        hbaseActionRows.getValue().put(Constants.USER_KEY, actionData[0]);
        Action action;
        //Need to append the scenario type to the Action Id, as each Scenario will check whether it has already
        //been matched before for that particular user (if applicable for that Scenario).
        if ( actionData.length == ACTION_TYPE_KEY_LENGTH ) {
            action = new Action(hbaseActionRows.getValue(), actionData[1].concat(Constants.HBASE_KEY_DELIMITER).concat(actionData[2]), Long.valueOf(actionData[actionData.length - 1]));
        } else {
            action = new Action(hbaseActionRows.getValue(), actionData[1], Long.valueOf(actionData[actionData.length - 1]));
        }
        return action;
    }

    public void logWrapper(String toLog) {
        hbaseClient.logWrapper(toLog);
    }

    public String fetchNewUserReferralCode(String userId,ArrayList<String> tables) {
        String referralCode="";
        String url="";
        deleteRowEntriesForExistingUser(userId, tables);
        return fetchReferralCodeForUser(userId);
    }

    public String fetchReferralCodeForUser(String userId) {
        return http.get("", getRubickCommonUrl() + userId + "/referralCode?type=install");
    }

    public void deleteRowEntriesForExistingUser(String userId,ArrayList<String> tables) {
        //delete all the existing rows if any
        try {
            for(String table:tables) {
                if (!fetchDbRowsForUserId(table.trim(), userId).isEmpty()) {
                    deleteHBaseRowsForUserId(table.trim(), userId);
                }
                if (!fetchDbRowsForUserId(table.trim(), userId).isEmpty()) {
                    deleteHBaseRowsForUserId(table.trim(), userId);
                }
            }
        }catch(Exception e) {
            logWrapper(e.getMessage());
        }
    }

    public void insertHbaseRowForActionId(JSONObject jsonObj) throws Exception {

        try {
            String actionId="";
            actionId=jsonObj.getString("actionId");
            String deviceAct=Constants._DEVICEACTIVATION;
//            switch(actionId) {
//                case "_DEVICEACTIVATION":
//                    insertDevActivationRow(jsonObj);
//                    break;
//                default:
//                    throw new Exception("No matching ActionId Found");
//            }
            if(actionId.equals(Constants._DEVICEACTIVATION)) {
                insertDevActivationRow(jsonObj);
            }
            else {
                throw new Exception("No matching ActionId Found");
            }
        } catch (JSONException e) {
            throw e;
        }
    }

    public void insertDevActivationRow(JSONObject jsonObj) throws Exception{

        String uniqueRequestId = UUID.randomUUID().toString();
        String userId=jsonObj.getString("userId");
        long timeStamp= Calendar.getInstance().getTimeInMillis();
        HashMap<String, String> map = new LinkedHashMap<String, String>();
        map.put("deviceId",fetchDeviceId());
        map.put("uniqueRequestId",UUID.randomUUID().toString());
        map.put("userId", jsonObj.getString("userId"));
        Action action=new Action(map,jsonObj.getString("actionId"),timeStamp);
        List<Action> listActions=Arrays.asList(action);
        hbaseClient.insertRows(Constants.HBASE_ACTION_TABLE, hbaseClient.getHbaseActionRows(listActions));
    }

    public String fetchDeviceId() {
        Random r = new Random( System.currentTimeMillis() );
        return (10000 + r.nextInt(20000))+"Test";
    }


    public void listenActionAPI_HttpPostCall(JSONObject jsonObj) throws Exception{
        String pl=jsonObj.getString("payLoad");
        String url=jsonObj.getString("url");
        http.post("calling listenAction api", url, pl);
        int status=http.getStatus("calling listenAction api",url,null,null);
        System.out.println("status recieved: " + status);
    }

    public JSONObject fetchPayloadForReferralApiPostCall(JSONObject jsonObj) throws Exception{

        return jsonObj;


    }

    public JSONObject fetchPayLoadForApplyReferral(JSONObject jsonObj) throws Exception {

        JSONObject returnJson=new JSONObject();
        JSONObject innerJson=new JSONObject();
        innerJson.put("deviceId",jsonObj.getString("deviceId"));
        innerJson.put("referralCode",jsonObj.getString("referralCode"));
        innerJson.put("referralType",jsonObj.getString("referralType"));

        returnJson.put("actionId","_APPLYREFERRALCODE");
        returnJson.put("attributeMap",innerJson.toString());
        return  returnJson;
    }


    public String fetchListenActionUrl(String userId,String prodUrl) {
        return prodUrl+userId+"/action";
    }

    public void putUserAndDeviceEntryInDeviceService(JSONObject jsonObj) throws Exception, DeviceServiceException {
        deviceService.putUserIdDeviceIdEntryInDeviceService(jsonObj);

    }

    public void deleteDeviceIdInfoInDeviceService(String deviceId) {
        deviceService.deleteDeviceIdInfoInDeviceService(deviceId);
    }


    public String getDeviceServiceUrl() {
        return deviceServiceUrl;
    }

    public void setDeviceServiceUrl(String deviceServiceUrl) {
        this.deviceServiceUrl = deviceServiceUrl;
    }

    public void fetchUserAndDevicePayload(JSONObject userAndDeviceInfo) throws JSONException {
        JSONObject payload=new JSONObject();
        payload.put("_appType","Retail");
        payload.put("appType","Retail");
        payload.put("_accountId",userAndDeviceInfo.get("user"));
        payload.put("accountId",userAndDeviceInfo.get("user"));
        payload.put("_loginTime",System.currentTimeMillis());
        payload.put("loginTime",System.currentTimeMillis());
        userAndDeviceInfo.put("payload",payload);
    }


    public void fetchListenLoginPayload(JSONObject userAndDeviceInfo) throws JSONException {
        JSONObject listenLoginPayload=new JSONObject();
        JSONObject data=new JSONObject();
        listenLoginPayload.put("accountId",userAndDeviceInfo.getString("user"));
        listenLoginPayload.put("deviceId",userAndDeviceInfo.getString("deviceId"));
        data.put("_loginTime",System.currentTimeMillis());
        listenLoginPayload.put("data",data);
        userAndDeviceInfo.put("loginPayload",listenLoginPayload);
    }

    public String callListenLoginApi(String loginPayload) {
        String url=getRubickCommonUrl()+"login";
        String response=http.post("listen login API call",url,loginPayload);
        return response;
    }

    public void cleanup(JSONObject inpArgsJson) throws JSONException {
        for(int i=0;i<inpArgsJson.getJSONArray("tablesList").length();i++) {
            for(int j=0;j<inpArgsJson.getJSONArray("usersList").length();j++) {
                deleteHBaseRowsForUserId(inpArgsJson.getJSONArray("tablesList").getString(i), inpArgsJson.getJSONArray("usersList").getString(j) + ":");
            }
        }
        for(int i=0;i<inpArgsJson.getJSONArray("devicesList").length();i++) {
            deleteDeviceIdInfoInDeviceService(inpArgsJson.getJSONArray("devicesList").getString(i));
        }
    }

    public void fetchInputArgsJson(JSONObject jsonObj) throws JSONException {
        int numUsers=Integer.valueOf(jsonObj.getInt("numUsers"));
        int numDevices=Integer.valueOf(jsonObj.getInt("numDevices"));
        int i;
        ArrayList<String> usersList= Lists.newArrayList();
        ArrayList<String> devicesList=Lists.newArrayList();

        for(i=0;i<numUsers;i++) {
            usersList.add(fetchRandomUserId()+i);
        }
        for(i=0;i<numDevices;i++) {
            devicesList.add(fetchDeviceId() + i);
        }
        jsonObj.put("usersList", usersList);
        jsonObj.put("devicesList", devicesList);
    }
}
