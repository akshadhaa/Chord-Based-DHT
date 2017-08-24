package edu.buffalo.cse.cse486586.simpledht;

import android.app.Application;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;

/**
 * Created by akshadha on 4/7/17.
 */

class ClientTask extends AsyncTask<String, Void, Void> {
    String[] updationMessage = {""};
    String[] updationMessage3 = {""};
    String[] updationMessage1= {""};
    String[] updationMessage4= {""};
    String[] updationMessage5= {""};
    String[] updationMessage6= {""};
    private final Uri mUri= SimpleDhtProvider.buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");





    @Override
    protected Void doInBackground(String... msgs) {
        try {

//             Context context=new Context() {
//             };

            String[] initMessage={};
            String msgToSend = msgs[0];
            String[] insertMsg ={};
            // String remotePort = portnumbers[i];

            if (msgToSend.contains("THISISTHEINITIALMESSAGE")) {
                Socket sock = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt("11108"));
                Log.d(SimpleDhtProvider.TAG, "CLIENT : I AM NOT 5554 and I CREATED A SOCKET TO CONTACT 5554");
                initMessage=msgToSend.split("THISISTHEINITIALMESSAGE");
                Log.d("CLIENT", "CLIENT: MESSAGETOSEND :" + initMessage[0]+ " " + SimpleDhtProvider.genHash(SimpleDhtProvider.myPortBy2));
                Log.d("CLIENT", "COMMUNICATING WITH 5554 : Sending message");
                PrintWriter pw = new PrintWriter(new OutputStreamWriter(sock.getOutputStream(), "UTF8"));
                Log.d("CLIENT", "CLIENT:PRINTWRITER OBJECT HAS BEEN CREATED");
                pw.write(initMessage[0] + "CONTACT5554" + SimpleDhtProvider.genHash(SimpleDhtProvider.myPortBy2)+"\n");
                pw.flush();


                Log.d("CLIENT", msgToSend + " CONTACT5554 " + SimpleDhtProvider.genHash(SimpleDhtProvider.myPortBy2));
                BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                String message = br.readLine();
                pw.close();

                Log.d(SimpleDhtProvider.TAG, "CLIENT : message received : " + message);
                if (message == null) {
                    SimpleDhtProvider.successor = SimpleDhtProvider.myPortBy2;
                    SimpleDhtProvider.predecessor = SimpleDhtProvider.myPortBy2;

                    Log.d("CLIENT", "CLIENT:SimpleDhtProvider.myPortBy2 : " + SimpleDhtProvider.myPortBy2);

                    Log.d("CLIENT", "5554 is not active," + SimpleDhtProvider.myPortBy2 + " is the only node active");
                    SimpleDhtProvider.isItSimple = true;
                } else if (message.contains("ACK")) {
                    Log.d("CLIENT : ", SimpleDhtProvider.myPortBy2 + " has received ack");

                    updationMessage1 = message.split("ACK");
                    Log.d("CLIENT", "CLIENT:5554 is not the only active node");
                    Log.d("CLIENT", "Client socket closing!");

                    sock.close();
                }
            }
            else if(msgToSend.contains("INSERTMESSAGECASEONE1")){

                Socket sock7 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(SimpleDhtProvider.successor)*2);

                Log.d("CLIENT_A", " INSERT MESSAGE : " + msgToSend.split("INSERTMESSAGECASEONE1")[0]);

                PrintWriter pw7 = new PrintWriter(new OutputStreamWriter(sock7.getOutputStream(), "UTF8"));
                pw7.write(msgToSend+"\n");
                pw7.flush();

                BufferedReader br7 = new BufferedReader(new InputStreamReader(sock7.getInputStream()));
                String message = br7.readLine();

//                if (message.contains("ACK")) {
//                    Log.d("CLIENT : ", SimpleDhtProvider.myPortBy2 + " has received ack in insert from " + msgs[1]);
//                    pw7.close();
//                    sock7.close();
//                }
                pw7.close();
                br7.close();
                sock7.close();

            }
            else if(msgToSend.contains("QUERYMESSAGECASEONE1")){

                Socket sock8 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(SimpleDhtProvider.successor)*2);
                Log.d("CLIENT_A", " QUERY MESSAGE1 : " + msgToSend);
                Log.d("CLIENT_A", " QUERY MESSAGE2 : " + msgToSend.split("QUERYMESSAGECASEONE1")[0]);

                PrintWriter pw8 = new PrintWriter(new OutputStreamWriter(sock8.getOutputStream(), "UTF8"));
                pw8.write(msgToSend+"\n");
                pw8.flush();
                pw8.close();
                sock8.close();
//                BufferedReader br7 = new BufferedReader(new InputStreamReader(sock7.getInputStream()));
//                String message = br7.readLine();

            }
            else if(msgToSend.contains("STARQUERY")){
                Socket sock9 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(SimpleDhtProvider.successor)*2);
                Log.d("CLIENT_A", " STAR QUERY MESSAGE1 : " + msgToSend);
                Log.d("CLIENT_A", " STAR QUERY MESSAGE2 : " + msgToSend.split("STARQUERY")[0]);

                PrintWriter pw9 = new PrintWriter(new OutputStreamWriter(sock9.getOutputStream(), "UTF8"));
                pw9.write(msgToSend+"\n");
                pw9.flush();
                pw9.close();
                sock9.close();

            }
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        } catch (UnsupportedEncodingException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }

                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
        return null;
    }
}

