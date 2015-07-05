package tests.engage.engage_api.rubick.src;

import com.flipkart.website.testrunner.Config;
import com.google.common.collect.Lists;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;
import tests.engage.engage_api.rubick.DAO.Action;
import tests.engage.engage_api.rubick.Service.Processor;
import tests.engage.engage_api.rubick.common.Constants;
import tests.engage.engage_api.rubick.common.exceptions.DeviceServiceException;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.flipkart.website.testng.Assertion.assertEquals;


/**
* Created by keshav.gupta on 15/05/15.
*/
public class Rubick {

    private String rubickCompletedActionsTableName="rubick.actions";
    private String rubickPendingActionsTableName="rubick.pendingActions";
    //private String prodUrl="http://engage-rubick.vip.nm.flipkart.com/referralService/v1/";
    private String prodUrl="http://rubick-app-master-0002.nm.flipkart.com:25481/referralService/v1/";
    private String deviceServiceUrl;
    private Processor processor;

    ArrayList<String> rubickDBTables= Lists.newArrayList();


    public Rubick() {
        Config config = new Config();
        config.loadConfigFile();
        deviceServiceUrl="http://mobile-services-preprod-0001.nm.flipkart.com:8000/v1/apps/retail/";
        //deviceServiceUrl=config.ConfigProperties.getProperty("mobileDeviceServiceUrl");

        processor=new Processor();
        processor.setDeviceServiceUrl(deviceServiceUrl);
        processor.setRubickCommonUrl(prodUrl);
        processor.instantiateDeviceServiceObject();
        rubickDBTables.add(rubickCompletedActionsTableName);
        rubickDBTables.add(rubickPendingActionsTableName);

        //System.out.println(config.ConfigProperties.getProperty("RubickProdUrl"));
//        processor.setRubickCommonUrl(config.ConfigProperties.getProperty("RubickProdUrl").trim());
    }

    @Test(groups="rubickTest")
    public void listenLoginAPI_sameUserDifferentDeviceId() {
        JSONObject inpArgs=new JSONObject();
        try {
            inpArgs.put("numUsers",1);
            inpArgs.put("numDevices",2);
            inpArgs.put("tablesList", rubickDBTables);
            processor.fetchInputArgsJson(inpArgs);
            processor.cleanup(inpArgs);
            JSONObject userAndDeviceInfo=new JSONObject();
            userAndDeviceInfo.put("user", inpArgs.getJSONArray("usersList").getString(0));
            userAndDeviceInfo.put("deviceId", inpArgs.getJSONArray("devicesList").getString(0));
            processor.fetchUserAndDevicePayload(userAndDeviceInfo);
            processor.putUserAndDeviceEntryInDeviceService(userAndDeviceInfo);
            userAndDeviceInfo.put("deviceId", inpArgs.getJSONArray("devicesList").getString(1));
            processor.fetchListenLoginPayload(userAndDeviceInfo);
            String listenLoginCallStatus=processor.callListenLoginApi(userAndDeviceInfo.getString("loginPayload"));
            List<Action> actions=processor.fetchDbRowsForUserId("rubick.actions", inpArgs.getJSONArray("usersList").getString(0));
            processor.cleanup(inpArgs);
            assertEquals(listenLoginCallStatus, "OK", "Expected OK, but found " + listenLoginCallStatus+" as response for listenLogin API call");
            assertEquals(actions.size(),0,"No Hbase rows should have been created");
        } catch (JSONException e) {
            e.printStackTrace();
        }  catch (DeviceServiceException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(groups="rubickTest")
    public void listenLoginAPI_SameUserMultipleAssociatedDevices() {
        JSONObject inpArgs=new JSONObject();
        try {
            inpArgs.put("numUsers",1);
            inpArgs.put("numDevices",2);
            inpArgs.put("tablesList", rubickDBTables);
            processor.fetchInputArgsJson(inpArgs);
            processor.cleanup(inpArgs);
            JSONObject userAndDeviceInfo=new JSONObject();
            userAndDeviceInfo.put("user", inpArgs.getJSONArray("usersList").get(0));
            userAndDeviceInfo.put("deviceId", inpArgs.getJSONArray("devicesList").get(0));
            processor.fetchUserAndDevicePayload(userAndDeviceInfo);
            processor.putUserAndDeviceEntryInDeviceService(userAndDeviceInfo);
            userAndDeviceInfo.put("deviceId", inpArgs.getJSONArray("devicesList").get(1));
            processor.fetchUserAndDevicePayload(userAndDeviceInfo);
            processor.putUserAndDeviceEntryInDeviceService(userAndDeviceInfo);
            processor.fetchListenLoginPayload(userAndDeviceInfo);
            String listenLoginCallStatus=processor.callListenLoginApi(userAndDeviceInfo.getString("loginPayload"));
            List<Action> actions=processor.fetchDbRowsForUserId("rubick.actions", (String) inpArgs.getJSONArray("usersList").get(0));
            processor.cleanup(inpArgs);
            assertEquals(listenLoginCallStatus, "OK", "Expected OK but found " + listenLoginCallStatus+" as response for listenLogin API call");
            assertEquals(actions.size(),0,"No Hbase rows should have been created");
        } catch (JSONException e) {
            e.printStackTrace();
        }  catch (DeviceServiceException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(groups="rubickTest")
    public void listenLoginAPI_ValidRequest() {
        JSONObject inpArgs=new JSONObject();
        try {
            inpArgs.put("numUsers", 1);
            inpArgs.put("numDevices", 1);
            inpArgs.put("tablesList", rubickDBTables);
            processor.fetchInputArgsJson(inpArgs);
            processor.cleanup(inpArgs);
            JSONObject userAndDeviceInfo=new JSONObject();
            userAndDeviceInfo.put("user", inpArgs.getJSONArray("usersList").getString(0));
            userAndDeviceInfo.put("deviceId", inpArgs.getJSONArray("devicesList").get(0));
            processor.fetchUserAndDevicePayload(userAndDeviceInfo);
            processor.putUserAndDeviceEntryInDeviceService(userAndDeviceInfo);
            processor.fetchListenLoginPayload(userAndDeviceInfo);
            String listenLoginCallStatus=processor.callListenLoginApi(userAndDeviceInfo.getString("loginPayload"));
            List<Action> actions = processor.fetchDbRowsForUserId("rubick.actions", (String) inpArgs.getJSONArray("usersList").get(0));
            assertEquals(actions.size(),1,"One "+Constants._DEVICEACTIVATION+" should have been inserted`");
            assertEquals(actions.get(0).getActionId(),Constants._DEVICEACTIVATION,"Expected Hbase DB row as "+Constants._DEVICEACTIVATION+" " +
                    "however found "+actions.get(0).getAttributeValue(""));
            assertEquals(actions.get(0).getAttributeValue("userId"), inpArgs.getJSONArray("usersList").getString(0),"UserId not found as expected" );
            assertEquals(actions.get(0).getAttributeValue("deviceId"),inpArgs.getJSONArray("devicesList").getString(0),"deviceId not found as expected" );
            processor.cleanup(inpArgs);
        } catch (JSONException e) {
            e.printStackTrace();
        }  catch (DeviceServiceException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(groups="rubickTest")
    public void listenAction_MissingActionId() {

    }


    @Test(groups = "p123")
    public void main1() {
        //HBaseClient hw= new HBaseClient();
        //String user="rer:";
        String user="tuser1:";
        try {

            processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,user);
//            for(int i=5;i<21;i++)
//                processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"wTest"+i);
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"user150009:_INCENTIVE");
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,user+":_INCENTIVE");
//            processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"usernew1:_SCENARIODETECTED");
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,user+":_APPLYREFERRALCODE");
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,user+":_DEVICEACTIVATION");
//            processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,user+":_SCENARIODETECTED:scenario_first_buy_referral");
//            processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,user+":_FIRSTBUY");
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"testUser:_INCENTIVE");
//            processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"utestER:_SCENARIODETECTED");
//            processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"utestER:_INCENTIVE");
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"userEE:_INCENTIVE");
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"ut");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(groups = "p123")
    public void main3() {
        try {
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"wtest4");
            //            JSONObject jsonObj=new JSONObject();
            //            jsonObj.put("userId", "wTest5");
            //            jsonObj.put("actionId", Constants._DEVICEACTIVATION);
            //            processor.insertHbaseRowForActionId(jsonObj);

            String actionId=Constants._DEVICEACTIVATION;
            String uniqueRequestId = UUID.randomUUID().toString();
            String userId="wTest5";
            long timeStamp= Calendar.getInstance().getTimeInMillis();
            timeStamp=timeStamp-TimeUnit.MILLISECONDS.convert(Long.parseLong("10"), TimeUnit.DAYS);
            HashMap<String, String> map = new LinkedHashMap<String, String>();
            map.put("deviceId", processor.fetchDeviceId());
            map.put("uniqueRequestId", UUID.randomUUID().toString());
            map.put("userId", userId);
            tests.engage.engage_api.rubick.DAO.Action action=new tests.engage.engage_api.rubick.DAO.Action(map,actionId,timeStamp);
            List<tests.engage.engage_api.rubick.DAO.Action> listActions=Arrays.asList(action);
            processor.hbaseClient.insertRows(Constants.HBASE_ACTION_TABLE, processor.hbaseClient.getHbaseActionRows(listActions));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(groups = "p123")
    public void main2() {
        try {
            //processor.deleteHBaseRowsForUserId(rubickCompletedActionsTableName,"wtest4");
            //Map<String, Map<String, HbaseColumn>> map=processor.fetchDbRowsForUserId("rubick.actions", "7d8cc838095e2338d8ca2e4fa5de1252");
            //System.out.println(map.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(groups = "p123")
    public void main77() {
        try {
            FileWriter fw = new FileWriter("./tests/data/resources/getRequests1.txt",true);
            for(int i=1;i<100001;i++) {
                fw.write("/rubickUser"+i+"/referralCode?type=global"+"\n");
                //fw.write("/rubickUser"+i+"/redemptionStatus"+"\n");
                fw.write("/incentiveProperties?key=scenario_referrer"+"\n");
                fw.write("/incentiveProperties?key=scenario_referral_install"+"\n");
                fw.write("/rubickUser"+i+"/dashboard?type=referincentives&markAsRead=true"+"\n");
                fw.write("/rubickUser"+i+"/redemptionStatus?deviceId="+(i+1000)+"\n");




            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    @Test(groups = "p123")
//    public void main5() {
////        long scenarioGracePeriodTimestamp = TimeUnit.MILLISECONDS.convert(Long.parseLong("7"), TimeUnit.DAYS);
////        System.out.println(scenarioGracePeriodTimestamp);
//
//        try {
//            String line="";
//            String line1="";
//            String line2="";
//            String pair="";
//            ArrayList list1=new ArrayList<>();
//            ArrayList<String> list2=new ArrayList<String>();
//            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("./tests/data/resources/getCode24june.txt")));
//            BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream("./tests/data/resources/rubUsersList.txt")));
//            FileWriter fw = new FileWriter("./tests/data/resources/pairold1.txt",true);
//            FileWriter fw1 = new FileWriter("./tests/data/resources/pairRandomnew1.txt",true);
//            fw.write("");
//            HashMap<String,String> userDevice=new HashMap<>();
//            while((line=br1.readLine())!=null) {
//                if(line.contains(",")) {
//                    String[] arr = line.split(",");
//                    //System.out.println(arr[0]+":"+arr[1]);
//                    userDevice.put(arr[0],arr[1]);
//                }
//
//            }
////            for(Map.Entry<String, String> entry : userDevice.entrySet()) {
////                String key = entry.getKey();
////                String value = entry.getValue();
////                System.out.println(key+":"+value);
////
////                // do what you have to do here
////                // In your case, an other loop.
////            }
//            while((line=br.readLine())!=null) {
//                //System.out.println(line+"\n");
//                if(line.contains("referralCode&")) {
//                    line1=line;
//                    line1=line1.replaceAll("<","");
//                    line1=line1.replaceAll(">","");
//                    line1=line1.replaceAll("responseData","");
//                    line1=line1.replaceAll("class","");
//                    line1=line1.replaceAll("java.lang.String","");
//                    line1=line1.replaceAll("\"","");
//                    line1=line1.replaceAll("&quot","");
//                    line1=line1.replaceAll("referralCode","");
//                    line1=line1.replaceAll(";","");
//                    line1=line1.replaceAll(":","");
//                    line1=line1.replaceAll("}","");
//                    line1=line1.replaceAll("\\{","");
//                    line1=line1.replaceAll(",","");
//                    line1=line1.replaceAll("/","");
//                    line1=line1.replaceAll("canRefer","");
//                    line1=line1.replaceAll("=","");
//                    line1=line1.replaceAll("true","");
//                    line1=line1.replaceAll("userId","");
//                    line1=line1.replaceAll("rubickUser.*","");
//                    list1.add(line1.trim());
//                    //line1=line1+",";
//
//                    //System.out.println(line1.trim() + "\n");
//                    //fw.write(line1.trim()+"\n");
//                }
//                if(line.contains("http://rubick-app-master-0004.nm.flipkart.com:25481/referralService")) {
//                    line2=line;
//                    line2=line2.replaceAll("<java.net.URL>http://rubick-app-master-0004.nm.flipkart.com:25481/referralService/v1/","");
//                    //line2=line2.replaceAll("/referralCode?type=install</java.net.URL>","");
//                    line2=line2.replaceAll("</java\\.net\\.URL>","");
//                    line2=line2.replaceAll("/referralCode","");
//                    line2=line2.replaceAll("type=global","");
//                    line2=line2.replaceAll("\\?","");
//
//
//                    line2=line2.trim();
//                    list2.add(line2.trim());
//                    line2=line2+","+userDevice.get(line2)+",";
//                    pair=line2+line1;
//                    System.out.println(pair);
//                    fw.write(pair + "\n");
//                    line1="";
//                    line2="";
//                    pair="";
//                }
//            }
//            System.out.println("list1Size: "+list1.size()+"\n");
//            System.out.println("list2Size: " + list2.size());
//            Collections.shuffle(list1);
//            for(long i=0;i<list1.size();i++) {
//                fw1.write(list2.get((int) i)+","+userDevice.get(list2.get((int) i))+","+list1.get((int) i)+"\n");
//
//            }
//            br.close();
//            br1.close();
//            fw.close();
//            fw1.close();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


    @Test(groups = "p123")
    public void main6() {
        try {
            FileWriter fw = new FileWriter("./tests/data/resources/rubickUserList.txt",true);
            for(long i=0;i<1000000;i++) {
                fw.write("user"+i+"\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test(groups="Functional")
    public void listenAction_MissingActionId_Test() {
        processor.logWrapper("Call listenAction API when 'actionId' key is missing from payload. Should return HTTP 400 status");

        String referee=processor.fetchRandomUserId();
        String referrer=processor.fetchRandomUserId();
        String referralCode=processor.fetchNewUserReferralCode(referrer, rubickDBTables);

        String referralCode2="";
        JSONObject jsonObj=new JSONObject();
        Map<String,Object> payLoad=new HashMap<String,Object>();
        payLoad.put("attributeMap",new HashMap<String,Object>());
        ((HashMap<String,Object>)payLoad.get("attributeMap")).put("deviceId", processor.fetchDeviceId());
        ((HashMap<String,Object>)payLoad.get("attributeMap")).put("referralCode", referralCode);
        ((HashMap<String,Object>)payLoad.get("attributeMap")).put("referralType", "install");
        try {
            jsonObj.put("url",processor.fetchListenActionUrl(referee,prodUrl));
            jsonObj.put("payLoad",payLoad);
            processor.listenActionAPI_HttpPostCall(jsonObj);

        } catch (JSONException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(groups="Functional")
    public void listenAction_MissingUserActivation_Test() {
        processor.logWrapper("User is not activated,validate exception message in the returned json");
        String referee=processor.fetchRandomUserId();
    }



    @Test(groups="Functional")
    public void getUserReferralCode_verifySameReferralCodeForExistingUser_Test() {

        processor.logWrapper("Make two calls to getUserReferralCode API for a new user.Verify referral" +
                             " code returned in the second call is same as the first one");
        String userId=processor.fetchRandomUserId();
        String referralCode1="";
        String referralCode2="";
        JSONObject jsonObj;
        try {
            jsonObj=new JSONObject(processor.fetchNewUserReferralCode(userId,rubickDBTables));
            referralCode1=jsonObj.getString("referralCode");
            jsonObj=new JSONObject(processor.fetchReferralCodeForUser(userId));
            referralCode2=jsonObj.getString("referralCode");
            processor.deleteRowEntriesForExistingUser(userId, rubickDBTables);
            processor.logWrapper("ReferralCode1: " + referralCode1 + "\n" + "ReferralCode2: " + referralCode2);
            assertEquals(referralCode1, referralCode2, "Different referral code returned for existing user");
        } catch (JSONException e) {
            assertEquals(true,false,"Different referral code returned for existing user");
            e.printStackTrace();
        }
    }

    @Test(groups="Functional")
    public void getUserReferralCode_verifyUniqueReferralCodeForMultipleUsers_Test() {
        processor.logWrapper("Make sure unique referral string is returned for 10 new users.");
        processor.logWrapper("\n-Create 10 usersIds (not production ids by any chance)");
        processor.logWrapper("\n-Delete rows if already for these users");
        processor.logWrapper("\n-Fetch referral Ids for each");
        processor.logWrapper("\n-Make sure all are unique");

        int counter=10;
        String userId;
        JSONObject jsonObj;
        ArrayList<String> referralCodeList=Lists.newArrayList();
        while(counter>0) {
            try {
                userId=processor.fetchRandomUserId();
                jsonObj=new JSONObject(processor.fetchNewUserReferralCode(userId,rubickDBTables));
                System.out.println(jsonObj.toString());
                processor.deleteRowEntriesForExistingUser(userId,rubickDBTables);
                referralCodeList.add(jsonObj.getString("referralCode"));
            } catch (JSONException e) {
                e.printStackTrace();
            }
            counter--;
        }
        Set<String> set=new HashSet<String>(referralCodeList);
        assertEquals(set.size(),referralCodeList.size(),"Found a duplicate Referral code for different users.");
    }


    @Test(groups="Functional")
    public void listenAction_APPLYREFERRALCODE_invalidReferralCode_Test() {
        processor.logWrapper("Make sure no DB rows are created ScenarioDetected and INCENTIVE keys for invalid Referral code." +
                "\n-Create a new userId (not production id by any chance,<14 Alpha numerics)" +
                "\n-Delete rows if already present for this user" +
                "\n-Directly insert DB row for actionId =_DEVICEACTIVATION"+
                "\n-Call listenAction API for actionIs = _APPLYREFERRALCODE passing invalid Referral code in payload, <5 alpha numerics" +
                "\n-Make sure no _SCENARIODETECTED and _INCENTIVE rows are created for user" +
                "\n-Check if _APPLYREFERRALCODE DB row is inserted" +
                "\n-Delete DB rows");

        String userId;
        try {
            userId=processor.fetchRandomUserId();
            processor.deleteRowEntriesForExistingUser(userId, rubickDBTables);

            JSONObject jsonObj=new JSONObject();
            jsonObj.put("userId", userId);
            jsonObj.put("actionId", Constants._DEVICEACTIVATION);
            processor.insertHbaseRowForActionId(jsonObj);

            Map<String,Object> map=new HashMap<String,Object>();
            map.put("actionId", Constants._APPLYREFERRALCODE);
            map.put("deviceId", processor.fetchDeviceId());
            //definitely an invalid referral code,since minimum length is 5
            map.put("referralCode", "aaa");
            map.put("referralType","install");

            //Action applyReferralObj=new Action(prodUrl+userId+"/action",map);

            //jsonObj=processor.fetchPayloadForReferralApiPostCall(applyReferralObj.fetchJsonObj());
            processor.listenActionAPI_HttpPostCall(jsonObj);
            processor.deleteHBaseRowsForUserId("rubick.actions",userId);

        }catch (Exception e) {
            e.printStackTrace();
        }

    }





}
