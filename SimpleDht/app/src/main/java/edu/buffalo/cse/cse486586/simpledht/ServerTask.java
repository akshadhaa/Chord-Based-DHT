package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by akshadha on 4/7/17.
 */

public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
    public  Uri mUri= SimpleDhtProvider.buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    static final String SERVER = SimpleDhtProvider.class.getSimpleName();
    public static List<String> kl;

    @Override
    protected Void doInBackground(ServerSocket... sockets) {
        String[] initialmessage = {""};
        String message = "";
        String left="";
        String right="";
        String[] updationMessage1= {""};
        String[] messageToUpdate0 ={""};
        String[] messageToUpdate1 ={""};
        String[] messageToUpdate2 ={""};
        String[] messageToUpdate3 ={""};

        String[] insertMsg1 = {""};
        String[] queryMsg1 ={""};
        String[] queryMsg3={""};
        String[] queryMsg4={""};


        ServerSocket serverSocket = sockets[0];
        try {
            while (true) {
                Log.d(SERVER, "SERVER LOG - FIRST MESSAGE ");
                Socket sock = serverSocket.accept();
                //Thread.sleep(1000);
                Log.d(SERVER, "SERVER LOG accepted socket connection");
                BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                Log.d(SERVER, "SERVER LOG -Buffered reader has been created");
                message = br.readLine();
                Log.d(SERVER, "SERVER LOG -MESSAGE RECEIVED :" + message);
                //if(message == null)
                    Log.d("NOTE: ", "The problem is here!");


                PrintWriter pw = new PrintWriter(new OutputStreamWriter(sock.getOutputStream(), "UTF8"));
                Log.d(SERVER,  SimpleDhtProvider.myPortBy2);
                if (message.contains("CONTACT5554")) {

                    if(SimpleDhtProvider.myPortBy2.equals("5554"))
                    {
                        Log.d(SimpleDhtProvider.TAG, "inside server, first isitsimple is " + SimpleDhtProvider.isItSimple);
                        SimpleDhtProvider.isItSimple = false;
                        Log.d(SimpleDhtProvider.TAG, "inside server, isitsimple is reset to" + SimpleDhtProvider.isItSimple);
                    }
                    Log.d(SERVER, "CODE CONTAINS CONTACT5554");
                    pw.write("ACK"+"\n");
                    pw.flush();
                    pw.close();
                    initialmessage = message.split("CONTACT5554");

                    Log.d(SERVER, "INITIAL MESSAGE of 0 is :" +initialmessage[0]);
                    Log.d(SERVER, "SERVER LOG - I'm 5554, and " + initialmessage[0] + "has contacted me");

                    SimpleDhtProvider.tm.put(SimpleDhtProvider.genHash(initialmessage[0]),initialmessage[0]);

                    Log.d(SERVER, "Size of active list:" + SimpleDhtProvider.activeList.size());
                    Set<String> keys = SimpleDhtProvider.tm.keySet();
                    kl = new ArrayList<String>(SimpleDhtProvider.tm.keySet());
                    /*
                    for (String key : keys) {
                        //String key=(String)it.next();
                        Log.d(SERVER,"KEY VALUE IS : " +key);
                    }
                    */
                    if(kl.size()==2) {
                        if (SimpleDhtProvider.successor.equals("5554") && SimpleDhtProvider.predecessor.equals("5554")) {

                            Log.d(SERVER, "SERVER SIDE : SUCCESSOR : IN IF LOOP :" + SimpleDhtProvider.successor);
                            Log.d(SERVER, "SERVER SIDE : PREDECESSOR : IN IF LOOP :" + SimpleDhtProvider.predecessor);

                            SimpleDhtProvider.successor = initialmessage[0];
                            SimpleDhtProvider.predecessor = initialmessage[0];

                            Log.d(SERVER, "IF 5554 is the only port, it's successor is " + SimpleDhtProvider.successor + " predecessor is :" + SimpleDhtProvider.predecessor);
                            Socket sock6 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(initialmessage[0])*2);
                            PrintWriter pw6 = new PrintWriter(new OutputStreamWriter(sock6.getOutputStream(), "UTF8"));
                            pw6.write(initialmessage[0] + "UPDATESUCCESSORPREDECESSOR\n");
                            pw6.flush();

                            BufferedReader br1 = new BufferedReader(new InputStreamReader(sock6.getInputStream()));
                            String ack_messg = br1.readLine();
                            Log.d(SimpleDhtProvider.TAG, "ack message received = " + ack_messg);
                            if (ack_messg.contains("ACK")) {
                                Log.d(SimpleDhtProvider.TAG, "ack received for sending first node update from 5554 to " + initialmessage[0]);
                                sock6.close();
                            }
                            Log.d(SERVER, "SERVER LOG : CHECKSUCPRED : " + SimpleDhtProvider.successor);
                            Log.d(SERVER, "SERVER LOG : CHECKSUCPRED : " + SimpleDhtProvider.predecessor);
                            //update at client side for myportby2ew
                        }
                    }

                    //   Log.d(SimpleDhtProvider.TAG, "SIZE OF KL : " +kl.size() + " SIZE OF MAP : " +SimpleDhtProvider.tm.size());
                    else if(kl.size()>2) {
                        Log.d(SERVER+"KLSIZE >2", "SIZE OF KL : " + kl.size());
                        //for(int j=0;j<kl.size();j++){
                        int j = kl.indexOf(initialmessage[1]);  //index of new node in the list
                        Log.d(SERVER+"KLSIZE >2", "HD initial message" + initialmessage[0]);
                        String key = kl.get(j);
                        String value = SimpleDhtProvider.tm.get(key);

                        Log.d(SimpleDhtProvider.TAG, "KEY VALUE IN ORDER IS : " + key);
                        if (j == 0) {
                            Log.d(SERVER+"KLSIZE >2", "KAJOL :VALUE OF j CASE 0 is " + j);
                            left = SimpleDhtProvider.tm.get(kl.get(kl.size() - 1));
                            right = SimpleDhtProvider.tm.get(kl.get(1));
                            Log.d(SERVER+"KLSIZE >2", "KAJOL :SIZE OF TM case 0 : " +SimpleDhtProvider.tm.size());
                            Log.d(SERVER+"KLSIZE >2","KAJOL :CASE 0 VALUE IS : " + value +" LEFT of j is : " +left +" RIght of j is : "+right );
                            communication(left, right, value);
                        } else if (j == kl.size() - 1) {
                            Log.d(SERVER+"KLSIZE >2","SIZE OF KL -1 FOR ROUTING THE MESSAGE : " +String.valueOf(kl.size()-1));
                            Log.d(SERVER+"KLSIZE >2", "KAJOL : VALUE OF j CASE 1 is : " + j);
                            left = SimpleDhtProvider.tm.get(kl.get(j-1));
                            right = SimpleDhtProvider.tm.get(kl.get(0));
                            Log.d(SERVER+"KLSIZE >2", "KAJOL :SIZE OF TM case 1 : " +SimpleDhtProvider.tm.size());
                            Log.d(SERVER+"KLSIZE >2","KAJOL :CASE 1 VALUE IS : " + value +" LEFT of j is : " +left +" RIght of j is : "+right );
                            communication(left, right, value);
                        } else {
                                Log.d(SimpleDhtProvider.TAG, "KAJOL :VALUE OF j CASE 2 is : " + j);
                                left = SimpleDhtProvider.tm.get(kl.get(j - 1));
                                right = SimpleDhtProvider.tm.get(kl.get(j + 1));
                                Log.d(SERVER+"KLSIZE >2", "KAJOL :SIZE OF TM case 2 : " +SimpleDhtProvider.tm.size());
                                Log.d(SERVER+"KLSIZE >2","KAJOL :CASE 2 VALUE IS : " + value +" LEFT of j is : " +left +" RIght of j is : "+right );
                                communication(left, right, value);
                        }
                        Log.d(SimpleDhtProvider.TAG, "KEY VALUE IS : " + key);
                    }

                }


                else if(message.contains("UPDATESUCCESSORPREDECESSOR")){
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "FIRST UPDATION : WHEN THERE's ONLY 1 NODE");
                    pw.write("ACK"+"\n");
                    pw.flush();
                    pw.close();
                    messageToUpdate0=message.split("UPDATESUCCESSORPREDECESSOR");
                    //communication("5554", "5554", messageToUpdate0[0]);
                    SimpleDhtProvider.predecessor = "5554";
                    SimpleDhtProvider.successor = "5554";
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "PORIKI : successor " +SimpleDhtProvider.successor );
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "PORIKI : predecessor " +SimpleDhtProvider.predecessor );
                }

                else if(message.contains("FIRSTUPDATEVALUE")){
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "updating client node");
                    pw.write("ACK"+"\n");
                    pw.flush();
                    pw.close();
                    messageToUpdate1=message.split("FIRSTUPDATEVALUE");
                    SimpleDhtProvider.predecessor=messageToUpdate1[0];
                    SimpleDhtProvider.successor=messageToUpdate1[1];
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "PORIKI : successor " +SimpleDhtProvider.successor );
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "PORIKI : predecessor " +SimpleDhtProvider.predecessor );
                }
                else if(message.contains("FIRSTUPDATELEFT")){
                    //update right for left
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "updating predecessor node");
                    pw.write("ACK"+"\n");
                    pw.flush();
                    pw.close();
                    messageToUpdate2=message.split("FIRSTUPDATELEFT");
                    SimpleDhtProvider.successor=messageToUpdate2[0];
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "PORIKI : successor " +SimpleDhtProvider.successor );
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "PORIKI : predecessor " +SimpleDhtProvider.predecessor );
                }
                else if(message.contains("FIRSTUPDATERIGHT")){
                    Log.d(SimpleDhtProvider.TAG, "updating successor node");
                    pw.write("ACK"+"\n");
                    pw.flush();
                    pw.close();
                    messageToUpdate3=message.split("FIRSTUPDATERIGHT");

                    SimpleDhtProvider.predecessor=messageToUpdate3[0];
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "PORIKI : successor " +SimpleDhtProvider.successor );
                    Log.d(SERVER+" UPDATING SUC AND PRED ", "PORIKI : predecessor " +SimpleDhtProvider.predecessor );
                }

                else if(message.contains("INSERTMESSAGECASEONE1")){
                    Log.d(SimpleDhtProvider.TAG, "inside insert in server " + SimpleDhtProvider.myPortBy2 + " the message is " + message);

                    insertMsg1=message.split("INSERTMESSAGECASEONE1");

                    Log.d("INSERT_A", "SERVER : Insert message " + insertMsg1[0]);

                    ContentValues keyValueToInsert = new ContentValues();
                    Log.d("inside insert in server", "cv object has been created");
                    //publishProgress(message);
                    //inserting <”key-to-insert”, “value-to-insert”>
                    keyValueToInsert.put("key", insertMsg1[0]);
                    Log.d("inside insert in server", "value of key : " +insertMsg1[0]);
                    keyValueToInsert.put("value", insertMsg1[1]);
                    Log.d("inside insert in server", "value : " +insertMsg1[1]);
                    Uri newUri = SimpleDhtActivity.context.getContentResolver().insert(
                            mUri,
                            keyValueToInsert
                   );



                }

                else if(message.contains("QUERYMESSAGECASEONE1")){
                    Log.d("QUERY_A","SERVER"+ "CLIENT HAS PASSED THE MESSAGE TO IT'S SUCCESSOR");
                    Log.d("QUERY_A", "MESSAGE RECEIVED : "+ message);
                    queryMsg1=message.split("QUERYMESSAGECASEONE1");

                    Log.d("QUERY_A", "MESSAGE KEY : "+ message);

                    //check if cursor object has data. if yes, send back to the guy who requested you

                    if(SimpleDhtProvider.sp.contains(queryMsg1[0])){

                        Log.d("QUERY_A", "I have the response for you : " + queryMsg1[0]);

                        String queryMsg2=queryMsg1[0]+"QUERYMESSAGECASETWO2"+SimpleDhtProvider.sp.getString(queryMsg1[0], "key");
                        Socket sock9 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(queryMsg1[1])*2);


                        Log.d("CLIENT_A", " QUERY MESSAGE : cursor is not null" + queryMsg1[1]);

                        PrintWriter pw9 = new PrintWriter(new OutputStreamWriter(sock9.getOutputStream(), "UTF8"));
                        pw9.write(queryMsg2+"\n");
                        pw9.flush();
                        pw9.close();
                        sock9.close();

                    } else {

                        Log.d("QUERY_A", "I don't have the response for you : " + queryMsg1[0]);

                        Socket sock9 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(SimpleDhtProvider.successor)*2);

                        PrintWriter pw9 = new PrintWriter(new OutputStreamWriter(sock9.getOutputStream(), "UTF8"));
                        pw9.write(message+"\n");
                        pw9.flush();
                        pw9.close();
                        sock9.close();

                    }
                }
                else if(message.contains("QUERYMESSAGECASETWO2")){
                    queryMsg3=message.split("QUERYMESSAGECASETWO2");
                    Log.d("QUERY_A", "Query Response : " + queryMsg3[0] + " Value : " + queryMsg3[1]);

                    SimpleDhtProvider.QueryKey = queryMsg3[0];
                    SimpleDhtProvider.QueryValue = queryMsg3[1];

                }

                else if(message.contains("STARQUERY")){
                    queryMsg4=message.split("STARQUERY");
                    String responseQuery="RESPONSETOSQUERY";

                    Log.d("QUERY_A","STAR QUERY SENDER : " +queryMsg4[0]);

                    if(!queryMsg4[0].equals(SimpleDhtProvider.myPortBy2)) {

                        for (int i = 0; i < SimpleDhtProvider.keyList.size(); i++) {
                            String localKey = SimpleDhtProvider.keyList.get(i);
                            String localValue = SimpleDhtProvider.sp.getString(SimpleDhtProvider.keyList.get(i), "key");

                            responseQuery += "@" + localKey + "&" + localValue;

                        }

                        Socket sock10 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(queryMsg4[0]) * 2);

                        PrintWriter pw9 = new PrintWriter(new OutputStreamWriter(sock10.getOutputStream(), "UTF8"));
                        pw9.write(responseQuery + "\n");
                        pw9.flush();
                        pw9.close();
                        sock10.close();
                        Log.d("QUERY_A", "RESPONSEQUERY : " + responseQuery);

                        Socket sock9 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(SimpleDhtProvider.successor) * 2);

                        PrintWriter pw19 = new PrintWriter(new OutputStreamWriter(sock9.getOutputStream(), "UTF8"));
                        pw19.write(message + "\n");
                        pw19.flush();
                        pw19.close();
                        sock9.close();
                    }

                }

                else if(message.contains("RESPONSETOSQUERY")){

                    Log.d("QUERY_A", "RESPNSEQUERY : MESSAGE RECEIVED AT SERVER : "+message);

                    String[] messages = message.split("@");

                    for(int i = 1; i < messages.length; i++){

                        Log.d("QUERY_A", "RESPNSEQUERY : MESSAGE RECEIVED AT SERVER : "+ messages[i]);

                        String localKey = messages[i].split("&")[0];
                        String localValue = messages[i].split("&")[1];

                        Log.d("QUERY_A", "RESPNSEQUERY : KEY/VALUE : " +  localKey + " : " + localValue);

                        SimpleDhtProvider.globalMatrixCursor.addRow(new String[]{localKey, localValue});


                    }

                    sock.close();

                }
//                    Log.d(SimpleDhtProvider.TAG, "inside insert in server " + SimpleDhtProvider.myPortBy2 + " the message is " + message);
//                    pw.write("ACK"+"\n");
//                    pw.flush();
//                    pw.close();
//                    String[] insertMsg1 = {""};
//
//                    insertMsg1=message.split("INSERTMESSAGECASEONE1");
//
//
//                    ContentValues keyValueToInsert = new ContentValues();
//                    //publishProgress(message);
//                    //inserting <”key-to-insert”, “value-to-insert”>
//                    keyValueToInsert.put("key", insertMsg1[0]);
//                    keyValueToInsert.put("value", insertMsg1[1]);
//
//                    Uri newUri = SimpleDhtActivity.context.getContentResolver().insert(
//                            mUri,
//                            keyValueToInsert
//                    );
//
//                }
//                else if(message.contains("INSERTMESSAGECASETWO2")){
//
//                    String[] insertMsg2 = {""};
//
//                    insertMsg2=message.split("INSERTMESSAGECASEONE1");
//                }
                Log.d(SimpleDhtProvider.TAG, "Size of active list : " + SimpleDhtProvider.activeList.size());

                sock.close();
            }


        }catch (IOException e) {
            Log.e(SimpleDhtProvider.TAG, "File read failed");
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } //catch (InterruptedException e) {
            //e.printStackTrace();
        //}
        //######
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
        return null;
    }

    protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
    }

    protected void communication(String left, String right, String value) throws IOException {
        Log.d(SERVER+"COMMUNUCATION METHOD","INSIDE COMMUNICAtiON METHOD with left: " + left + " right " + right + " value " + value);
//        String pred=left;
//        String suc = right;
//        String node=value;

        Socket sock1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(value)*2);
        Log.d(SERVER+"COMMUNUCATION METHOD", "Socket connection to " + Integer.parseInt(value)*2 + " from " + SimpleDhtProvider.myPortBy2 +" status " + sock1.isConnected());
        PrintWriter pw1 = new PrintWriter(new OutputStreamWriter(sock1.getOutputStream(), "UTF8"));
        String messageSending = left+"FIRSTUPDATEVALUE"+right+"\n";
        Log.d(SERVER+"COMMUNUCATION METHOD", "String being sent is: " + messageSending);
        pw1.write(messageSending);
        pw1.flush();
        BufferedReader br = new BufferedReader(new InputStreamReader(sock1.getInputStream()));
        if(br.readLine().equals("ACK")) {
            Log.d(SERVER+"COMMUNUCATION METHOD", "ack received for new node join. Socket closing....");
            sock1.close();
        }

        if(!left.equals(SimpleDhtProvider.myPortBy2) && !right.equals(SimpleDhtProvider.myPortBy2)) {
            Socket sock2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(left) * 2);
            //update right for left;
            PrintWriter pw2 = new PrintWriter(new OutputStreamWriter(sock2.getOutputStream(), "UTF8"));
            String messageSending2 = value + "FIRSTUPDATELEFT\n";
            pw2.write(messageSending2);
            pw2.flush();
            sock2.close();

            Socket sock3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(right) * 2);
            //update left for right
            PrintWriter pw3 = new PrintWriter(new OutputStreamWriter(sock3.getOutputStream(), "UTF8"));
            String messageSending3 = value + "FIRSTUPDATERIGHT\n";
            pw3.write(messageSending3);
            pw3.flush();
            sock3.close();

            pw2.close();
            pw3.close();
        }

        else if((Integer.parseInt(left) == Integer.parseInt(SimpleDhtProvider.myPortBy2)))
        {
            Log.d(SERVER+"COMMUNUCATION METHOD", "HD 5554 is the predecessor to be updated");
            SimpleDhtProvider.successor = value;
            Log.d(SimpleDhtProvider.TAG, "PORIKI: now predecessor is:" + SimpleDhtProvider.predecessor);
            Log.d(SimpleDhtProvider.TAG, "PORIKI: successor update for 5554:" + SimpleDhtProvider.successor);
            Socket sock3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(right) * 2);
            //update left for right
            PrintWriter pw3 = new PrintWriter(new OutputStreamWriter(sock3.getOutputStream(), "UTF8"));
            String messageSending3 = value + "FIRSTUPDATERIGHT\n";
            pw3.write(messageSending3);
            pw3.flush();
            sock3.close();
        }
        else if(Integer.parseInt(right) == Integer.parseInt(SimpleDhtProvider.myPortBy2))
        {
            Log.d(SERVER+"COMMUNUCATION METHOD", "HD 5554 is the successor to be updated");
            SimpleDhtProvider.predecessor = value;
            Log.d(SimpleDhtProvider.TAG, "PORIKI: predecessor update for 5554:" + SimpleDhtProvider.predecessor);
            Log.d(SimpleDhtProvider.TAG, "PORIKI: now successor is:" + SimpleDhtProvider.successor);
            Socket sock2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(left) * 2);
            //update right for left;
            PrintWriter pw2 = new PrintWriter(new OutputStreamWriter(sock2.getOutputStream(), "UTF8"));
            String messageSending2 = value + "FIRSTUPDATELEFT\n";
            pw2.write(messageSending2);
            pw2.flush();
            sock2.close();
        }
        pw1.close();
        return;
    }
}
