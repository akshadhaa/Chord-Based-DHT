package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.preference.PreferenceManager;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {


    private final Uri mUri= SimpleDhtProvider.buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
    public static boolean isItSimple=false;
    public static TreeMap<String,String> tm=new TreeMap<String, String>();
    static final String TAG = SimpleDhtProvider.class.getSimpleName();

   // static final String[] portnumbers={"11108","11112","11116","11120","11124"};
    static final int SERVER_PORT = 10000;
    String myPort="";
    public static String myPortBy2="";
    String hashKey="";
    public static ArrayList<String> keyList = new ArrayList<String>();
    HashMap<String, String> hm= new HashMap<String, String>();
    public static ArrayList<String> activeList=new ArrayList<String>();
    ArrayList<String> al=new ArrayList<String>();
    HashMap<String, String> hmStatic1= new HashMap<String, String>();
    HashMap<String, String> hmStatic2= new HashMap<String, String>();
    public static String predecessor="";
    public static String successor="";
    public static boolean firstGuy = false;
    public static String QueryKey = "";
    public static String QueryValue = "";
    public static SharedPreferences sp;
    public static MatrixCursor globalMatrixCursor;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d(TAG, "DELETE HAS BEEN CALLED");
        // TODO Auto-generated method stub
        if(sp.contains(selection)){
            SharedPreferences.Editor edt = sp.edit();
            edt.remove(selection);
            edt.commit();
            keyList.remove(selection);
        }



        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public  Uri insert(Uri uri, ContentValues values) {
        //Log.d("SDHT : INSERT", "VALUE OF isitsimple : " +isItSimple);
        // TODO Auto-generated method stub

        try {
            if (isItSimple) {
                Log.d("SDHT : ISITSIMPLE", "values.getAsString(\"value\") " + values.getAsString("value"));
                Log.d("SDHT : ISITSIMPLE", "values.getAsString(\"key\")" + values.getAsString("key"));
                SharedPreferences.Editor edt = sp.edit();
                edt.putString(values.getAsString("key"), values.getAsString("value"));
                Log.d(SimpleDhtProvider.TAG, "INSERT : KEY " + values.getAsString("key"));
               keyList.add(values.getAsString("key"));
                Log.d("SDHT : ISITSIMPLE", "SIZE OF KEYLIST IS : " + keyList.size());
                Log.d("SDHT : ISITSIMPLE", "INSERT : VALUE " + values.getAsString("value"));
                edt.commit();
                // *********
                Log.v("insert", values.toString());

                // return null;
            } else {

                Log.d("SDHT : INSERT", "VALUE OF isitsimple : " +isItSimple);
                String keyMsg = values.getAsString("key");
                String hashMsg = genHash(keyMsg);
                String msg = values.getAsString("value");
                Log.d("INSERT : " , "KEY IS : " + keyMsg);

                String insertMessage=keyMsg+"INSERTMESSAGECASEONE1"+msg;

                Log.d("SDHT : insert ", "before if : ");

                if(genHash(myPortBy2).compareTo(genHash(predecessor))<0){
                    firstGuy = true;
                }


                Log.d("INSERT_A", "FIRST GUY : " + firstGuy);

                Log.d("INSERT_A", "KEY RECEIVED : " + values.getAsString("key"));

                Log.d("INSERT_A", "My PORT : " + myPortBy2);


                if((hashMsg.compareTo(genHash(predecessor))>0 && hashMsg.compareTo(genHash(myPortBy2))<=0) ||

                         ( firstGuy && (hashMsg.compareTo(genHash(predecessor))>0 || (hashMsg.compareTo(genHash(myPortBy2)))<=0)))

                 {   Log.d("INSERT_A", "KEY goes in my DB : " + values.getAsString("key"));
                     Log.d("SDHT", "hash(MSG) is > pred and less than successor");
                     Log.d("SDHT myport:insert " ,myPortBy2 +" hash "+genHash(myPortBy2));
                     Log.d("SDHT PRED:insert ",predecessor + " hash "+genHash(predecessor));
                     Log.d("SDHT succ:insert ",successor + " hash "+genHash(successor));

                     Log.d("SDHT: NOT SIMPLE :", "INSERT CASE 0 : Message has to be saved to myport :" +hashMsg);
//                     String insertHashMsg = keyMsg + "INSERTMESSAGECASEONE1" + msg;  

                     SharedPreferences.Editor edt = sp.edit();
                     edt.putString(keyMsg,msg);
                     edt.commit();
                     keyList.add(values.getAsString("key"));


                     Log.d(SimpleDhtProvider.TAG, " ABCD : INSERT : KEY " + values.getAsString("key"));
                     Log.d(SimpleDhtProvider.TAG, "ABCD : pred" + genHash(predecessor));
                     Log.d(SimpleDhtProvider.TAG, "ABCD : myport" + genHash(myPortBy2));


                  //   Log.d(SimpleDhtProvider.TAG, " ABCD : INSERT : KEY " + values.getAsString("key"));
                     Log.d(SimpleDhtProvider.TAG, "INSERT : VALUE " + values.getAsString("value"));

                     Log.d("SDHT : INSERT : key :", values.getAsString("key") + " values : " +values.getAsString("value"));
//                   //  keyList.add(values.getAsString("key"));
//                     Log.d("SDHT : NOT SIMPLE", "SIZE OF KEYLIST IS : " + keyList.size());
//                     Log.d("SDHT : NOT SIMPLE", "INSERT : VALUE " + values.getAsString("value"));
                     //edt.commit();
                 }
                 else {

//                     Log.d("INSERT_A", "KEY DROPPED : " + values.getAsString("key"));
//                     Log.d("INSERT_A", "KEY DROPPED HASH : " + hashMsg + " MY Hash : " + genHash(myPortBy2));
                     Log.d("SDHT INSERT : " , "INSERT CASE 2 : GREATER THAN MYPORT");
                     Log.d("INSERT_A", "KEY goes in my SUCCESSOR : " + values.getAsString("key"));
                     new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMessage, myPort);

                 }

            }
        }catch(NoSuchAlgorithmException e) {
            Log.e("INSERT_A", "Error : " +  e);
        }catch (Exception e){
            Log.e("INSERT_A", "Error : " +  e);
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        // SharedPreferences sp1 = getContext().getSharedPreferences("hash_table_name", 4);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPortBy2 = String.valueOf((Integer.parseInt(portStr)));

        sp = getContext().getSharedPreferences("hash_table_name", 4);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e("SDHT ONCREATE :", "Can't create a ServerSocket");
        }


        Log.d("SDHT ONCREATE ", "ONCREATE- MYPORT : " + myPort);
        Log.d("SDHT ONCREATE ", "ONCREATE- MYPORT by 2: " + myPortBy2);

        try {
            if (myPortBy2.equals("5554")) {
                successor = "5554";
                predecessor = "5554";
                //activeList.add("5554");
                //Collections.sort(SimpleDhtProvider.activeList.subList(1, SimpleDhtProvider.activeList.size()));
                tm.put(genHash("5554"), "5554");
                Log.d("SDHT ONCREATE :", "PORIKI : successor " +SimpleDhtProvider.successor );
                Log.d("SDHT ONCREATE :", "PORIKI : predecessor " +SimpleDhtProvider.predecessor );
                Log.d("SDHT ONCREATE" , "Size of active list, if 5554 is active : " + activeList.size());
                isItSimple = true;
            } else if (!myPortBy2.equals("5554")) {
                String message123 = myPortBy2 + "THISISTHEINITIALMESSAGE";
                Log.d("SDHT ONCREATE :", "I'm not 5554, and my portby2 value is : " + myPortBy2);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message123, myPort);
                Log.d("SDHT ONCREATE :", "Size of active list, if I'm not 5554: " + SimpleDhtProvider.activeList.size());
            }

        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        int count=0;

        String filter = selection;
        Log.d("SDHT : QUERY", "VALUE OF SELECTION IS : " +filter);
        Log.d("SDHT : QUERY", "size of active list : " +activeList.size());


        //if (isItSimple) {
            if (filter.equals(("@"))) {
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                for (int i = 0; i < keyList.size(); i++) {
                    Log.d("SDHT : QUERY", "Querying for all ");

                  //  if(keyList.get(i).compareTo(genHash(myPortBy2))<0)

                    matrixCursor.addRow(new String[]{keyList.get(i), sp.getString(keyList.get(i), "key")});
                    Log.d("SDHT : SIMPLE QUERY ", "QUERY123 : KEY IS : " + keyList.get(i));
                    Log.d("SDHT : SIMPLE QUERY", "QUERY123 : VALUE IS : " + sp.getString(keyList.get(i), "key"));
                    count++;
                    Log.d("SDHT : SIMPLE QUERY", "VALUE345 OF COUNT IS : " + count);
                    Log.d("SDHT : SIMPLE QUERY", "VALUE345 OF i is " + i);


                    // Map<String,String> keys= PreferenceManager.getDefaultSharedPreferences().getAll();
                    // *****
                    //Log.v("query", selection);
                }

                return matrixCursor;

            } else if (filter.equals("*")) {

                Log.d("QUERY*_A", "Case hit, start");

                globalMatrixCursor = new MatrixCursor(new String[]{"key", "value"});

                Log.d("QUERY*_A", "Sending a query to my successor.");
                String queryMessageStar=myPortBy2+"STARQUERY";

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,queryMessageStar , myPort);

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Log.d("QUERY_A", "My exceptions");
                }


                Log.d("QUERY*_A", "Others have populated their keys, I am adding mine.");

                for (int i = 0; i < keyList.size(); i++) {
                    globalMatrixCursor.addRow(new String[]{keyList.get(i), sp.getString(keyList.get(i), "key")});
                }


                return globalMatrixCursor;
            } else {
                Log.d("SDHT : NOT SIMPLE QUERY", "Querying only 1 key");
                Log.d("SDHT : NOT SIMPLE QUERY", "QUERYING FOR : " + filter);

               if(sp.contains(filter)){
                   String value = sp.getString(filter, "key");
                   Log.d("QUERY_A", "I have the value for the key : " + value + "and i am " +myPortBy2);
                   MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                   matrixCursor.addRow(new String[]{filter, value});
                   return matrixCursor;
               } else {
                   String queryMessage=filter+"QUERYMESSAGECASEONE1"+myPortBy2;

                   Log.d("QUERY_A", "I don't have the key. Asking the successor!");

                   new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,queryMessage , myPort);


                   try {
                       Thread.sleep(500);
                   } catch (InterruptedException e) {
                       Log.d("QUERY_A", "My exceptions");
                   }

                   Log.d("QUERY_A", "I have the value for the key from some node: " + QueryValue);
                   MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                   matrixCursor.addRow(new String[]{QueryKey, QueryValue});
                   return matrixCursor;

               }

            }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public static String genHash(String input) throws NoSuchAlgorithmException {
       // Log.d("SDHT : GENHASH","GENHASH : input " +input);
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


   public static int sequence=0;

}



