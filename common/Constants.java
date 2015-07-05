package tests.engage.engage_api.rubick.common;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by nitin.kaushik on 31/03/15.
 */
public class Constants {

    public static final String COLUMN_FAMILY_A = "a";

    public static final String REFERRAL_CODE_TABLE_COLUMN_FAMILY = "a";
    public static final String ACTION_TABLE_COLUMN_FAMILY = "a";
    public static final String CHARACTER_SET_TABLE_COLUMN_FAMILY = "a";
    public static final String REFERRAL_CODE_COUNTER_TABLE_COLUMN_FAMILY = "a";
    public static final String REFERRAL_CODE_HBASE_KEY = "referral_code";
    public static final String USER_ID_HBASE_KEY = "user_id";
    public static final String REFERRAL_CODE_COUNTER_COLUMN_QUALIFIER = "counter";
    public static final String CHARACTER_SET_TABLE_ROW_KEY = "characterSet";
    public static final String CHARACTER_SET_TABLE_COLUMN_QUALIFIER = "rubickReferralCode";
    public static final String PENDING_TASK_TABLE_COLUMN_FAMILY = "a";
    public static final String GENERATE_REFERRAL_CODE_ACTION_STRING = "_GENERATE_REFERRALCODE";
    public static final String HBASE_KEY_DELIMITER = ":";
    public static final String USER_KEY = "userId";
    public static final String SCENARIO_KEY = "scenarioId";
    public static final String ACTION_ID_KEY = "action";
    public static final String TIMESTAMP_KEY = "timestamp";
    public static final String REFERRAL_CODE_KEY = "referralCode";
    public static final String REFERRAL_TYPE_KEY = "referralType";
    public static final String ACTION_UID = "uniqueRequestId";
    public static final String DEVICE_ID_KEY = "deviceId";
    public static final String INCENTIVES_KEY = "incentives";
    public static final String INCENTIVE_PROPERTIES_KEY = "incentiveProperties";
    public static final String REDEMPTION_STATUS_KEY = "canRedeem";
    public static final String REDEMPTION_EXPIRY_TIME_KEY = "expires";

    public static final int USER_ID_INDEX_IN_ACTIONS_TABLE_KEY = 1;
    public static final int ACTION_TYPE_INDEX_IN_ACTIONS_TABLE_KEY = 2;

    public static final String REFERRAL_INSTALL_REFERRAL_TYPE = "install";

    public static final String HBASE_ACTION_TABLE = "rubick.actions";
    public static final String HBASE_PENDING_ACTIONS_TABLE = "rubick.pendingActions";

    //types of actions, includes both explicit and implicit actions
    public static final String _DEVICEACTIVATION = "_DEVICEACTIVATION";
    public static final String _SCENARIO_DETECTED = "_SCENARIODETECTED";
    public static final String _APPLYREFERRALCODE = "_APPLYREFERRALCODE";
    public static final String _DEVICELOGIN = "_DEVICELOGIN";

    public static final String COLUMN_USER_ID = "user_id";
    public static final String COLUMN_ACTION_TYPE = "action_type";
    public static final String COLUMN_REFERRAL_CODE = "referral_code";
    public static final String COLUMN_REFERRAL_TYPE = "referral_type";

    public static final String COLUMN_METADATA = "column_metadata";

    public static final String _INCENTIVE = "_INCENTIVE";

    public static final String REFER_INCENTIVE_TYPE = "REFER_INCENTIVE";

    //All common qualifiers related to an incentive
    public static final String INCENTIVE_QUALIFIER_TYPE = "incentiveType";
    public static final String INCENTIVE_QUALIFIER_ACOUNT_ID = "accountId";
    public static final String INCENTIVE_QUALIFIER_CREATED_DATE = "createdDate";
    public static final String INCENTIVE_QUALIFIER_END_DATE = "expiryDate";

    //All  qualifiers related to EGV
    public static final String EGV_QUALIFIER_INCENTIVE_AMOUNT = "denomination";
    public static final String EGV_QUALIFIER_CODE = "code";
    public static final String EGV_QUALIFIER_PIN = "pin";
    public static final String EGV_QUALIFIER_EXPIRY_DATE = "expiryDate";

    //All qualifiers related to SantaOffer
    public static final String SANTA_QUALIFIER_DISCOUNT_ID = "targetedDiscountId";

    public static final String DASHBOARD_REFER_INCENTIVE_INFO = "referincentivesInfo";
    public static final String DASHBOARD_TYPE = "type";
    public static final String EGV_CLIENT_ID_HEADER = "Flipkart-Gifting-Client-Id";
    public static final String EGV_CLIENT_TOKEN_HEADER = "Flipkart-Gifting-Client-Token";

    private static final String VALID_ACTIONS_KEY = "ACCEPT_ACTIONS";
    private static final String CONFIG_FILE = "/etc/fk-w3-rubick/rubick.properties"; //TODO make sure to add this file in post install script
    public static Set<String> VALID_ACTIONS = Sets.newHashSet();
    public static String SANTA_BASE_URL = "";
    public static String EGV_BASE_URL = "";
    public static Map<String, String> EGV_CLIENT_HEADERS_MAP = Maps.newHashMap();

    //Constants for User Notifications
    public static final String WELCOME_NOTIFICATION_TRACKINGID = "";
    public static final String WELCOME_NOTIFICATION_TITLE = "Welcome to flipkart!";
    public static final String WELCOME_NOTIFICATION_TEXT = "Enter referral code to win rewards";
    public static final String WELCOME_NOTIFICATION_LINK = "/";
    public static final String WELCOME_NOTIFICATION_IMAGE_LINK = "/";
    public static final long WELCOME_NOTIFICATION_EXPIRE_TIME = 0L;
    public static final long WELCOME_NOTIFICATION_SHOW_TIME = 0L;

    public static final String REFERRAL_NOTIFICATION_TRACKINGID = "";
    public static final String REFERRAL_NOTIFICATION_TITLE = "Hola!";
    public static final String REFERRAL_NOTIFICATION_TEXT = "Congrats on installing and winning";
    public static final String REFERRAL_NOTIFICATION_LINK = "/";
    public static final String REFERRAL_NOTIFICATION_IMAGE_LINK = "/";
    public static final long REFERRAL_NOTIFICATION_EXPIRE_TIME = 0L;
    public static final long REFERRAL_NOTIFICATION_SHOW_TIME = 0L;

    public static final String REFERRED_NOTIFICATION_TRACKINGID = "";
    public static final String REFERRED_NOTIFICATION_TITLE = "Welcome to flipkart!";
    public static final String REFERRED_NOTIFICATION_TEXT = "Your friend entered your referral code";
    public static final String REFERRED_NOTIFICATION_LINK = "/";
    public static final String REFERRED_NOTIFICATION_IMAGE_LINK = "/";
    public static final long REFERRED_NOTIFICATION_EXPIRE_TIME = 0L;
    public static final long REFERRED_NOTIFICATION_SHOW_TIME = 0L;


    static {
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(CONFIG_FILE));
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        String value = properties.getProperty(VALID_ACTIONS_KEY);
        for ( String action : value.split(",") ) {
            VALID_ACTIONS.add(action.trim());
        }

        SANTA_BASE_URL = properties.getProperty("santa.baseURL").trim();
        //remove / in the end if present
        if ( SANTA_BASE_URL.endsWith("/") ) {
            SANTA_BASE_URL.subSequence(0, SANTA_BASE_URL.length() - 1);
        }

        EGV_BASE_URL = properties.getProperty("egv.baseURL").trim();
        //remove / in the end if present
        if ( EGV_BASE_URL.endsWith("/") ) {
            EGV_BASE_URL.subSequence(0, EGV_BASE_URL.length() - 1);
        }
        EGV_CLIENT_HEADERS_MAP.put(EGV_CLIENT_ID_HEADER, properties.getProperty("egv.clientid").trim());
        EGV_CLIENT_HEADERS_MAP.put(EGV_CLIENT_TOKEN_HEADER, properties.getProperty("egv.clienttoken").trim());
    }

}
