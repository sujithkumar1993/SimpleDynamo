package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.acl.LastOwnerException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    static final String QUERY = "QUERY";
    static final String INSERT = "INSERT";
    static final String REPLICA = "REPLICA";
    static final String REPLICAQUERY = "REPLICAQUERY";
    static final String INSERT_CORDINATOR = "INSERT_CORDINATOR";
    static final String DELETE = "DELETE";
    static final String RECOVER = "RECOVER";
    static final String delim = "###";
    static String myPort;
    String failedPort = "";
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    final String[] AVD_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    TreeMap<String, String> nodeAndHashMap = new TreeMap<String, String>();
    ArrayList<String> activeNodesList = new ArrayList<String>();
    Object lock = new Object();
    Object insertlock = new Object();

    @Override
    public boolean onCreate() {
        Log.e(TAG, "Inside OnCreate...");

        try {
            nodeAndHashMap.put(genHash("5554"), "11108");
            nodeAndHashMap.put(genHash("5556"), "11112");
            nodeAndHashMap.put(genHash("5558"), "11116");
            nodeAndHashMap.put(genHash("5560"), "11120");
            nodeAndHashMap.put(genHash("5562"), "11124");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        Log.e("ONCREATE", "nodeAndHashMap..." + nodeAndHashMap);

        try {

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.e("ONCREATE", "Server thread created...");


        } catch (IOException e) {
            e.printStackTrace();
            Log.e("ONCREATE", "Can't Start Servertask");

        }

        TelephonyManager tel = (TelephonyManager) getContext().getApplicationContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        recoverData();
        return false;


    }

    public synchronized void recoverData(){
        String[] ports = {"11120","11116","11108","11112","11124"};

        Log.e("RECOVER","********INSIDE RECOVER DATA*******");
        try {

            for(int i=0;i<5;i++){
                if(!myPort.equals(ports[i])){
                    Log.e("RECOVER","********SENDING RECOVERY PING TO******* "+ports[i]);
                    String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, RECOVER, ports[i], "@").get();
                    Log.e("RECOVER","********RECOVER DATA FROM******* "+ports[i]+" "+response);

                    if(!(response == null||response.isEmpty()||response.toString().equals("{}"))){

                        JSONObject js = new JSONObject(response);
                        Iterator<String> keyiter = js.keys();

                        while (keyiter.hasNext()) {
                            String key = keyiter.next();
                            String hashedKey = "";
                            try {

                                hashedKey = genHash(key);
                            } catch (NoSuchAlgorithmException e) {
                            }
                            Log.e("RECOVER","hasedkey ***"+hashedKey);

                            activeNodesList.clear();
                            activeNodesList.addAll(nodeAndHashMap.keySet());

                            int nodehash = Integer.parseInt(myPort)/2;
                            int index = activeNodesList.indexOf(genHash(String.valueOf(nodehash)));
                            Log.e("RECOVER","index *** "+index);
                            String pred1 = nodeAndHashMap.get(activeNodesList.get((index+3)%5));
                            String pred2 = nodeAndHashMap.get(activeNodesList.get((index+4)%5));
                            String node = "";
                            if (hashedKey.compareTo(genHash("5562")) > 0 && hashedKey.compareTo(genHash("5556")) <= 0)
                            {
                                node = "5556";
                            }
                            else if (hashedKey.compareTo(genHash("5556")) > 0 && hashedKey.compareTo(genHash("5554")) <= 0)
                            {
                                node = "5554";
                            }
                            else if (hashedKey.compareTo(genHash("5554")) > 0 && hashedKey.compareTo(genHash("5558")) <= 0)
                            {
                                node = "5558";
                            }
                            else if (hashedKey.compareTo(genHash("5558")) > 0 && hashedKey.compareTo(genHash("5560")) <= 0)
                            {
                                node = "5560";
                            }
                            else if (hashedKey.compareTo(genHash("5560")) > 0 && hashedKey.compareTo(genHash("5562")) <= 0)
                            {
                                node = "5562";
                            }
                            else node = "5562";

                            node = String.valueOf(Integer.parseInt(node)*2);
//                        boolean b0 = genHash(myPort).equals(activeNodesList.get(0))&&hashedKey.compareTo(nodeAndHashMap.get(activeNodesList.get(4)))>0;
//                        boolean b4 = genHash(myPort).equals(activeNodesList.get(0))&&hashedKey.compareTo(nodeAndHashMap.get(activeNodesList.get(0)))>0;
//                        boolean b1 = hashedKey.compareTo(nodeAndHashMap.get(activeNodesList.get((index+2)%5)))>0&&(hashedKey.compareTo(nodeAndHashMap.get(activeNodesList.get((index+3)%5)))<=0);
//                        boolean b2 = hashedKey.compareTo(nodeAndHashMap.get(activeNodesList.get((index+3)%5)))>0&&hashedKey.compareTo(nodeAndHashMap.get(activeNodesList.get((index+4)%5)))<=0;
//                        boolean b3 = hashedKey.compareTo(nodeAndHashMap.get(activeNodesList.get((index+4)%5)))>0&&hashedKey.compareTo(nodeAndHashMap.get(activeNodesList.get((index+5)%5)))<=0;

                            Log.e("RECOVER","key ***"+key);
                            Log.e("RECOVER","node ***"+node);
                            Log.e("RECOVER","pred1 ***"+pred1);
                            Log.e("RECOVER","pred2 ***"+pred2);



                            if(node.equals(myPort)||node.equals(pred1)||node.equals(pred2)){
                                synchronized (lock){
                                String[] localfiles = getContext().fileList();
                                List<String> locallist = Arrays.asList(localfiles);
                                    Log.e("RECOVERDATA","LOCAL LIST "+Arrays.toString(locallist.toArray()));

                                    if(locallist.contains(key)){
                                    Log.e("RECOVERDATA","KEY EXISTS CONDITION PASSSED *** "+key);
                                    String recovervalue = js.getString(key).split(delim)[0];
                                    Long recovertime = Long.parseLong(js.getString(key).split(delim)[1]);
                                        Log.e("RECOVERDATA","Recover key *** "+key);
                                    Log.e("RECOVERDATA","Recover value###time *** "+recovervalue+delim+recovertime);
                                        FileInputStream fileInput = getContext().getApplicationContext().openFileInput(key);

                                        BufferedReader b = new BufferedReader(new InputStreamReader(fileInput));
                                        String localdata = "";
                                        localdata = b.readLine();
                                        Log.e("RECOVERDATA","LOCAL data *** "+localdata);

                                        String[] l = localdata.split("###");
                                        String localvalue = l[0];
                                        Long localtime = Long.parseLong(l[1]);
                                        Log.e("RECOVERDATA","l[0] *** "+l[0]);
                                        Log.e("RECOVERDATA","l[1] *** "+l[1]);
                                        Log.e("RECOVERDATA","LOCAL VALUE *** "+localvalue+delim+localtime);
                                        b.close();
                                        Log.e("RECOVERDATA","Local value###time *** "+localvalue+delim+localtime);
                                    if(recovertime > localtime){
                                        FileOutputStream outputStream;
                                        outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
                                        outputStream.write((recovervalue+delim+recovertime).getBytes());
                                        Log.e("RECOVERDATA", "Recover File write success in " + myPort);
                                        outputStream.close();
                                    }
                                    else{
                                        FileOutputStream outputStream;
                                        outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
                                        outputStream.write((localvalue+delim+localtime).getBytes());
                                        Log.e("RECOVERDATA", "Recover File write success in " + myPort);
                                        outputStream.close();
                                    }
                                }
                                else{
                                    FileOutputStream outputStream;
                                    outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
                                    Log.e("RECOVER","NEW KEY CONDITION PASSSED ***");
                                    String value = js.getString(key);
                                    outputStream.write(value.getBytes());
                                    Log.e("RECOVER", "Recover File write success in " + myPort);
                                    outputStream.close();
                                }}
                            }

                        }}

                }}
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }


    public synchronized void delete_local_files() {

        String[] files = getContext().fileList();
        for (String filetoread : files) {
            Log.e("DELETE", "File to delete ****: " + filetoread);
            try {
                getContext().deleteFile(filetoread);
                Log.e("DELETE", "File DELETE SUCCESS: " + filetoread);
            } catch (Exception e) {
                Log.e("DELETE", "File DELETE FAILURE: " + filetoread);
                e.printStackTrace();
            }
        }
    }

    @Override
    public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.e("DELETE", "Inside DELETE...");
        if (selection.equals("@")) {
            delete_local_files();
        } else if (selection.equals("*")) {
            delete_local_files();
            for (String i : AVD_PORTS) {
                if (!myPort.equals(i)) {
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, DELETE, i, "@");
                }
            }
        } else {
            try {
                getContext().deleteFile(selection);
                for (String i : AVD_PORTS) {
                    if (!myPort.equals(i)) {
//                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, DELETE, i, selection);
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, DELETE, i, "@");
                    }
                }

            } catch (Exception e) {
                Log.e("DELETE", "DELETE FAILED...");
                e.printStackTrace();
            }

        }


        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    public synchronized Uri insert_replica(Uri uri, ContentValues values) {
        try {
            Log.e("INSERT", "Inside insert replica method...");
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            FileOutputStream outputStream;
            outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            Log.e(TAG, "File write success in " + myPort);
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed in " + myPort);
        }
        return uri;
    }

    public void send_replicas(String port1, String port2, String key, String value) {

        try {
            if (myPort.equals(port1)) {
                FileOutputStream outputStream;
                outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                Log.e(TAG, "here 1 Replica File write success in " + myPort);
                outputStream.close();
                Log.e(TAG, "here 1 Sending replica to " + port2);
                String resp =new  ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA, port2, key + delim + value).get();
                Log.e(TAG, "here 1 Sending replica status of " + port2+" "+resp);
            } else if (myPort.equals(port2)) {
                FileOutputStream outputStream;
                outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                Log.e(TAG, "here 2 Replica File write success in " + myPort);
                outputStream.close();
                Log.e(TAG, "here 2 Sending replica to " + port1);
                String resp = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA, port1, key + delim + value).get();
                Log.e(TAG, "here 2 Sending replica status of " + port1+" "+resp);

            } else {
                Log.e(TAG, "here 3 Sending replica to " + port1);
                String resp = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA, port1, key + delim + value).get();
                Log.e(TAG, "here 3 Sending replica status of " + port1+" "+resp);
                Log.e(TAG, "here 3 Sending replica to " + port2);
                String resp1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA, port2, key + delim + value).get();
                Log.e(TAG, "here 3 Sending replica status of " + port2+" "+resp1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Log.e(TAG, "send_replicas**** Error while sending replicas from " + myPort);

        }


    }

    //***************************************** INSERT *******************************************************


    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        synchronized (lock){
            activeNodesList.clear();
            activeNodesList.addAll(nodeAndHashMap.keySet());}

        Log.e("INSERT", "*******activeNodesList: " + activeNodesList);
        Log.e("INSERT", "Inside Insert method");
        String key = values.getAsString("key");
        String value = values.getAsString("value")+delim+System.currentTimeMillis();
        Log.e("INSERT", "Key: " + key + " Value: " + value);
        String hashedKey = "";
        try {

            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
        }


        try {
            if (hashedKey.compareTo(activeNodesList.get(0)) < 0 || hashedKey.compareTo(activeNodesList.get(activeNodesList.size() - 1)) > 0) {

                Log.e("INSERT", "COMPARE: " + hashedKey.compareTo(activeNodesList.get(0)));
                Log.e("INSERT", "COMPARE: " + hashedKey.compareTo(activeNodesList.get(activeNodesList.size() - 1)));
                String toPort = nodeAndHashMap.get(activeNodesList.get(0));
                String rep1 = nodeAndHashMap.get(activeNodesList.get(1));
                String rep2 = nodeAndHashMap.get(activeNodesList.get(2));

                if (myPort.equals(toPort)) {
                    Log.e("INSERT", "here1");
                    Log.e("INSERT", "toPort: " + toPort);
                    Log.e("INSERT", "replica1: " + rep1);
                    Log.e("INSERT", "replica2: " + rep2);
                    Log.e("INSERT", "Sending LOCAL INSERT to " + toPort + " rep1: " + rep1 + " rep2: " + rep2);
                    try {
                        synchronized (insertlock){
                        FileOutputStream outputStream;
                        outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
                        outputStream.write(value.getBytes());
                        Log.e(TAG, "File write success in " + myPort);
                        outputStream.close();}
                        //						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA,rep1,key+delim+value).get();
                        //						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA,rep2,key+delim+value).get();
                        send_replicas(rep1, rep2, key, value);
                    } catch (Exception e) {
                        Log.e(TAG, "File write failed in " + myPort);
                    }

                } else {
                    Log.e("INSERT", "here2");
                    Log.e("INSERT", "Sending INSERT to " + toPort + " rep1: " + rep1 + " rep2: " + rep2);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT_CORDINATOR, toPort, key + delim + value).get();
                    //					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA,rep1,key+delim+value).get();
                    //					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA,rep2,key+delim+value).get();
                    send_replicas(rep1, rep2, key, value);

                }
            } else {
                Log.e("INSERT", "here3");
                for (int i = 0; i < activeNodesList.size(); i++) {
                    if (hashedKey.compareTo(activeNodesList.get(i)) > 0 && hashedKey.compareTo(activeNodesList.get(i + 1)) <= 0) {
                        Log.e("INSERT", "here4");
                        Log.e("INSERT", "INDEX: " + i);
                        String toPort = nodeAndHashMap.get(activeNodesList.get(i + 1));
                        String rep1 = nodeAndHashMap.get(activeNodesList.get((i + 2) % 5));
                        String rep2 = nodeAndHashMap.get(activeNodesList.get((i + 3) % 5));
                        Log.e("INSERT", "toPort: " + toPort);
                        Log.e("INSERT", "replica1: " + rep1);
                        Log.e("INSERT", "replica2: " + rep2);

                        if (myPort.equals(toPort)) {
                            try {
                                synchronized (insertlock){
                                Log.e("INSERT", "INSERT LOCAL " + toPort);
                                FileOutputStream outputStream;
                                outputStream = getContext().getApplicationContext().openFileOutput(key, Context.MODE_PRIVATE);
                                Log.e(INSERT, "before insert value :" + value);
                                outputStream.write(value.getBytes());
                                Log.e(INSERT, "File write success in " + myPort);
                                Log.e(INSERT, "Key :" + key);
                                Log.e(INSERT, "after insert value :" + value);
                                outputStream.close();}
                                //							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA,rep1,key+delim+value).get();
                                //							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA,rep2,key+delim+value).get();
                                send_replicas(rep1, rep2, key, value);

                                break;
                            } catch (Exception e) {
                                Log.e(TAG, "File write failed in " + myPort);
                            }
                        } else {
                            Log.e("INSERT", "here5");
                            Log.e("INSERT", "Sending INSERT to " + toPort);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT_CORDINATOR, toPort, key + delim + value).get();
                            //					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA,rep1,key+delim+value).get();
                            //					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICA,rep2,key+delim+value).get();
                            send_replicas(rep1, rep2, key, value);

                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();

        }


        return uri;
    }

    public Cursor query_local_key(String selection) {
        Log.e("QUERY", "*****INSIDE LOCAL KEY-WISE QUERY****");

        String filetoread = selection;
        Log.e("QUERY", "FILE TO QUERY: " + filetoread);

        try {
            FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
            BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
            String[] row = new String[2];
            row[0] = selection;
            String[] values = br.readLine().split(delim);
            br.close();
            row[1]=values[0];
            String[] columns = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columns);
            mc.addRow(row);
            Log.e("QUERY", "query success");
            Log.e("QUERY", "key: " + row[0] + " value: " + row[1]+" actualvalue: "+values[0]+delim+values[1]);
            return mc;
        } catch (Exception e) {
            Log.e("QUERY", "ERROR WHILE LOCAL QUERY");
            e.printStackTrace();

        }
        return null;
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        Log.e("QUERY", "INSIDE QUERY METHOD..");
        Log.e("QUERY", "selection parameter: " + selection);
        if (selection.equals("@")) {

            String[] columns = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columns);
            String[] files = getContext().fileList();
            for (String filetoread : files) {
                Log.e("QUERY", selection+" File name: " + filetoread);
                try {
                    FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
                    String[] row = new String[2];
                    row[0] = filetoread;
                    String[] values = br.readLine().split(delim);
                    br.close();
                    row[1] =values[0];
                    Log.e("QUERY", "key: " + row[0] + " value: " + row[1]+" actualvalue: "+values[0]+delim+values[1]);
                    mc.addRow(row);

                } catch (Exception e) {
                    e.printStackTrace();
                }


            }
            return mc;

        } else if (selection.equals("*")) {
            String[] columns = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columns);
            String[] files = getContext().fileList();
            for (String filetoread : files) {
                Log.e("QUERY", "File name: " + filetoread);
                try {
                    FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
                    String[] row = new String[2];
                    row[0] = filetoread;
                    String[] values = br.readLine().split(delim);
                    br.close();
                    row[1] = values[0];
                    mc.addRow(row);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            for (String i : AVD_PORTS) {
                try {
                    if (!i.equals(myPort)) {
                        String response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY, i, "@").get();
                        Log.e("QUERY", selection+" QUERY RESPONSE FROM "+i +": "+ response);
                        JSONObject js = new JSONObject(response);
                        Iterator<String> keyiter = js.keys();
                        while (keyiter.hasNext()) {
                            String key = keyiter.next();
                            try {
                                String[] row = new String[2];
                                String[] values = js.getString(key).split(delim);
                                row[0] = key;
                                row[1] = values[0];
                                mc.addRow(row);
                            } catch (JSONException e) {
                                Log.e("QUERY", "EXCEPTION PARSING JSON Object");
                            }
                        }

                    }
                } catch (Exception e) {
                    Log.e("QUERY", "Exception while sending QUERY req to " + i);
                    e.printStackTrace();
                }
            }
            return mc;
        } else {
            String[] columns = {"key", "value"};
            MatrixCursor mc = new MatrixCursor(columns);
            Log.e("QUERY", "KEY: " + selection);
            String hashedKey = null;

            try {
                hashedKey = genHash(selection);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            //			Cursor c = query_local_key(selection);
            //			if(c != null){
            //				Log.e("QUERY","********************HERE QUERY*********************** "+c);
            //				return c;
            //			}

            try {
                if (hashedKey.compareTo(activeNodesList.get(0)) < 0 || hashedKey.compareTo(activeNodesList.get(activeNodesList.size() - 1)) > 0) {
                    String toPort = nodeAndHashMap.get(activeNodesList.get(0));
                    String rep1 = nodeAndHashMap.get(activeNodesList.get(1));
                    String rep2 = nodeAndHashMap.get(activeNodesList.get(2));
                    if (myPort.equals(toPort) || myPort.equals(rep1) || myPort.equals(rep2)) {
                        Log.e("QUERY", "here1");
                        return query_local_key(selection);

                    } else {


                        try {
                            TreeMap<Long,String> valuemap = new TreeMap<Long, String>();
                            Log.e("QUERY", "Sending QUERY to " + rep2);
                            String response1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICAQUERY, rep2, selection).get();
                            if(!(response1 ==null||response1.isEmpty()||response1.toString().equals("{}"))){
                                JSONObject js1 = new JSONObject(response1);
                                valuemap.put(Long.parseLong(js1.getString(selection).split(delim)[1]),js1.getString(selection).split(delim)[0]);
                            }
                            Log.e("QUERY", "here2 Sending QUERY to replica1 " + rep1);
                            String response2 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICAQUERY, rep1, selection).get();
                            if(!(response2 ==null||response2.isEmpty()||response2.toString().equals("{}"))){
                                JSONObject js2 = new JSONObject(response2);
                                valuemap.put(Long.parseLong(js2.getString(selection).split(delim)[1]),js2.getString(selection).split(delim)[0]);
                            }
                            Log.e("QUERY", "here2 Sending QUERY to " + toPort);
                            String response3 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY, toPort, selection).get();
                            if(!(response3 ==null||response3.isEmpty()||response3.toString().equals("{}"))){
                                JSONObject js3 = new JSONObject(response3);
                                valuemap.put(Long.parseLong(js3.getString(selection).split(delim)[1]),js3.getString(selection).split(delim)[0]);
                            }
                            Log.e("QUERY"," "+valuemap);
//                                    Log.e("QUERY", "QUERY RESPONSE " + response);
//                                    JSONObject js = new JSONObject(response);
                            try {
                                String[] row = new String[2];
                                row[0] = selection;
                                row[1] = valuemap.get(valuemap.lastKey());
                                mc.addRow(row);
                            } catch (Exception e) {
                                Log.e("QUERY", "EXCEPTION PARSING JSON Object. here4");
                            }

                            return mc;

                        } catch (Exception e) {
                            Log.e("QUERY", "Exception while sending QUERY req to " + toPort);
                            e.printStackTrace();
                        }
                    }
                } else {

                    for (int i = 0; i < activeNodesList.size(); i++) {
                        if (hashedKey.compareTo(activeNodesList.get(i)) > 0 && hashedKey.compareTo(activeNodesList.get((i + 1)%5)) <= 0) {
                            String toPort = nodeAndHashMap.get(activeNodesList.get((i + 1)%5));
                            String rep1 = nodeAndHashMap.get(activeNodesList.get((i + 2) % 5));
                            String rep2 = nodeAndHashMap.get(activeNodesList.get((i + 3) % 5));

                            if (myPort.equals(toPort) || myPort.equals(rep1) || myPort.equals(rep2)) {
                                Log.e("QUERY", "here3");
                                return query_local_key(selection);
                            } else {
                                Log.e("QUERY", "here4");
                                Log.e("QUERY","selection "+selection);

                                try {
                                    TreeMap<Long,String> valuemap = new TreeMap<Long, String>();
                                    Log.e("QUERY", "Sending QUERY to " + rep2);
                                    String response1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICAQUERY, rep2, selection).get();
                                    if(!(response1 ==null||response1.isEmpty()||response1.toString().equals("{}"))){
                                        JSONObject js1 = new JSONObject(response1);
                                        valuemap.put(Long.parseLong(js1.getString(selection).split(delim)[1]),js1.getString(selection).split(delim)[0]);
                                    }
                                    Log.e("QUERY", "here2 Sending QUERY to replica1 " + rep1);
                                    String response2 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPLICAQUERY, rep1, selection).get();
                                    if(!(response2 ==null||response2.isEmpty()||response2.toString().equals("{}"))){
                                        JSONObject js2 = new JSONObject(response2);
                                        valuemap.put(Long.parseLong(js2.getString(selection).split(delim)[1]),js2.getString(selection).split(delim)[0]);
                                    }
                                    Log.e("QUERY", "here2 Sending QUERY to " + toPort);
                                    String response3 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY, toPort, selection).get();
                                    if(!(response3 ==null||response3.isEmpty()||response3.toString().equals("{}"))){
                                        JSONObject js3 = new JSONObject(response3);
                                        valuemap.put(Long.parseLong(js3.getString(selection).split(delim)[1]),js3.getString(selection).split(delim)[0]);
                                    }
                                    Log.e("QUERY"," "+valuemap);
//                                    Log.e("QUERY", "QUERY RESPONSE " + response);
//                                    JSONObject js = new JSONObject(response);
                                        try {
                                            String[] row = new String[2];
                                            row[0] = selection;
                                            row[1] = valuemap.get(valuemap.lastKey());
                                            mc.addRow(row);
                                        } catch (Exception e) {
                                            Log.e("QUERY", "EXCEPTION PARSING JSON Object. here4");
                                        }

                                    return mc;

                                } catch (Exception e) {
                                    Log.e("QUERY", "Exception while sending QUERY req to " + toPort);
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


        }


        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.e(TAG, "*** INSIDE ServerTask METHOD ***");
            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {


                while (true) {
                    try {
                        Log.e(TAG, "*** WAITING TO ACCEPT ***");
                        Socket socket = serverSocket.accept();
                        Log.e("ST", "***ServerTask --------Connection accepted by server***");
                        BufferedReader brIncomingMsg = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        PrintWriter outputWriter = new PrintWriter(socket.getOutputStream(), true);
                        String incomingString = brIncomingMsg.readLine();
                        String[] a = incomingString.split(delim);
                        String requestType = a[0];
                        Log.e("ST", "***request PAYLOAD*** " + incomingString);
                        Log.e("ST", "***request type IN SERVER*** " + requestType);

                        if (requestType.equals(INSERT)) {
                            DataOutputStream cout = new DataOutputStream(socket.getOutputStream());
                            String s = "INSERTOK";
                            cout.writeUTF(s);
                            cout.flush();
                            cout.close();
                            Log.e("ST", "INSERT request received in serverTask...");
                            Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider”");
                            ContentValues cv = new ContentValues();
                            cv.put("key", a[2]);
                            cv.put("value", a[3]+delim+a[4]);
                            insert(uri, cv);

                        } else if (requestType.equals(REPLICA) || requestType.equals(INSERT_CORDINATOR)) {
                            synchronized (insertlock){
                                FileOutputStream os;
                                os = getContext().getApplicationContext().openFileOutput(a[2], Context.MODE_PRIVATE);
                                String v1 = a[3];
                                String v2 = a[4];
                                String v = v1+delim+v2;
                                os.write(v.getBytes());
                                os.close();
                                Log.e("ST", "File write success in " + myPort);
                                DataOutputStream cout = new DataOutputStream(socket.getOutputStream());
                                String s = "REPLICAOK";
                                cout.writeUTF(s);
                                cout.flush();
                                cout.close();
                            }
//                            Log.e("ST", "REPLICA request received...");
//                            Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider”");
//                            ContentValues cv = new ContentValues();
//                            cv.put("key", a[2]);
//                            cv.put("value", a[3]);

//                            insert_replica(uri, cv);


                        }
//                        else if (requestType.equals(QUERY)||requestType.equals(REPLICAQUERY)) {
//                            Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider”");
//
//                            String fromPort = a[1];
//                            String selection = a[2];
//                            Log.e("ST", "QUERY request received from... " + fromPort);
//                            Cursor cursor;
//                            if(selection.equals("@")){
//                                cursor = query(uri,null,selection,null,null);
//                            }
//                            else{
//                                cursor = query_local_key(selection);
//                            }
//
//                            JSONObject data = new JSONObject();
//                            Log.e("ST", "***CURSOR RECEIVED*** " + cursor);
//                            if (cursor != null && cursor.moveToFirst()) {
//                                while (!cursor.isAfterLast()) {
//                                    String key = cursor.getString(cursor.getColumnIndex("key"));
//                                    String value = cursor.getString(cursor.getColumnIndex("value"));
//                                    data.put(key, value);
//                                    cursor.moveToNext();
//                                }
//                            }
//                            DataOutputStream cout = new DataOutputStream(socket.getOutputStream());
//                            Log.e("ST", "***JSON DATA*** " + data);
//                            cout.writeUTF(data.toString());
//                            cout.flush();
//                            Log.e("ST", "***QUERY*** " + cursor);
//                            try {
//                                cursor.close();
//                            } catch (NullPointerException e) {
//                                Log.e("CURSOR", "CURSOR EXCEPTION");
//                                e.printStackTrace();
//                            }
//                        }

                        else if(requestType.equals(QUERY)||requestType.equals(REPLICAQUERY)){
                            String fromPort = a[1];
                            String selection = a[2];
                            JSONObject data = new JSONObject();
                            if(selection.equals("@")) {
                                String[] files = getContext().fileList();
                                for (String filetoread : files) {
                                    Log.e("QUERY", selection + " File name: " + filetoread);
                                    try {
                                        FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
                                        BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
                                        String value = "";
                                        value = br.readLine();
                                        br.close();
                                        data.put(filetoread,value);
                                        Log.e("QUERY", "key: " + filetoread + " value: " + value);

                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }}}
                            else{
                                FileInputStream fileInput = getContext().getApplicationContext().openFileInput(selection);
                                BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
                                String value = "";
                                value= br.readLine();
                                br.close();
                                data.put(selection,value);
                            }
                            DataOutputStream cout = new DataOutputStream(socket.getOutputStream());
                            Log.e("ST", "***JSON DATA*** " + data);
                            cout.writeUTF(data.toString());
                            cout.flush();
                        }

                        else if(requestType.equals(RECOVER)){
                            //(requestType.equals(new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, RECOVER, AVD_PORTS[i], "@");))
                            Log.e("ST", "***RECOVER REQUEST FROM*** " + a[1]);
                            String[] files = getContext().fileList();
                            JSONObject data = new JSONObject();
                            for (String filetoread : files) {
                                try {
                                    FileInputStream fileInput = getContext().getApplicationContext().openFileInput(filetoread);
                                    BufferedReader br = new BufferedReader(new InputStreamReader(fileInput));
                                    String key = filetoread;
                                    String value = br.readLine();
                                    br.close();
                                    data.put(key,value);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }


                            }
                            DataOutputStream cout = new DataOutputStream(socket.getOutputStream());
                            Log.e("ST", "***RECOVERJSON DATA*** " + data);
                            cout.writeUTF(data.toString());
                            cout.flush();


                        }

                        else if(requestType.equals(DELETE)){
                            synchronized (lock){
                                Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider”");
                                String selection = a[2];
                                delete(uri,selection,null);
                                DataOutputStream cout = new DataOutputStream(socket.getOutputStream());
                                Log.e("ST", "***DELETE key= " + selection);
                                String l = "DELETEOK";
                                cout.writeUTF(l);
                                cout.flush();
                                cout.close();
                            }
                        }






                    } catch (Exception e) {
                        e.printStackTrace();
                        Log.e("ST", "Error while socket connection in Server ", e);
                    }


                }


            } catch (Exception e) {
                e.printStackTrace();
                Log.e("ST", "Error while socket connection in Server ", e);

            }
            return null;
        }


    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {

            //            new queryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,QUERY,succPort,selection);
            Log.e("CT", "******INSIDE CLIENT TASK****** ");
            String requestType = msgs[0];
            String toPort = msgs[1];
            Log.e("CT", "Request Type: " + requestType);
            Log.e("CT", " TO: " + toPort);
            Log.e("CT", " FROM: " + myPort);
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(toPort));
                socket.setSoTimeout(2000);
                PrintWriter outmessage = new PrintWriter(socket.getOutputStream(), true);
                String textToSend = requestType + delim + myPort + delim + msgs[2];
                outmessage.println(textToSend);
                DataInputStream brIncomingMsg = new DataInputStream(socket.getInputStream());
                String response = brIncomingMsg.readUTF();
                brIncomingMsg.close();
                Log.e("CT", "******RESPONSE ****** " + response);
                return response;


            } catch (SocketTimeoutException e) {
                e.printStackTrace();
                return null;
            } catch (SocketException e) {
                e.printStackTrace();
                return null;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            catch (Exception e){
                e.printStackTrace();
                return null;
            }

        }
    }
}


