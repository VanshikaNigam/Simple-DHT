package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
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


public class SimpleDhtProvider extends ContentProvider {


    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    String myPort = null;
    String portStr = null;
    String predecessor = null;
    String successor = null;
    String pred_hash = null;
    String succ_hash = null;
    String key = null;
    String hashed_port = null;
    String value = null;
    String type_of_work[] = {"New", "Join", "Insert", "Msg_After_Join", "New Successor", "New Predecessor", "Forward", "Got_Value_for_Key", "Retrieve","Forward Delete"};
    HashMap<String, String> port_and_hash = new HashMap<String, String>();
    List<String> node_id = new ArrayList<String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        String selection_hashed="";
        try {
            selection_hashed = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(myPort.equals(successor)&& myPort.equals(predecessor))
        {
            if (selection.equals("*") || selection.equals("@"))
            {
                String files[] = getContext().fileList();
                for(String list:files)
                getContext().deleteFile(list);
            }
            else
                getContext().deleteFile(selection);
        }


        else if(selection.equals("@"))
        {
            String files[] = getContext().fileList();
            for(String list:files)
                getContext().deleteFile(list);
        }

        else if ((selection_hashed.compareTo(pred_hash) > 0) && selection_hashed.compareTo(hashed_port) < 0)
        {
            getContext().deleteFile(selection);
        }

        else if (isFirstNode() && (selection_hashed.compareTo(pred_hash) > 0 || selection_hashed.compareTo(hashed_port) < 0))
        {
            getContext().deleteFile(selection);
        }


        else {
            String msg_forward_delete = portStr + "#" + type_of_work[10] + "#" + successor + "#" + selection;

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_forward_delete);
            Log.d("Delete", msg_forward_delete);
           // String msg[]=msg_confirm.split("#");
               //getContext().deleteFile(selection);

            //getContext().deleteFile(selection);
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO:INSERT FUNCTION CONTENT PROVIDER

        key = values.getAsString("key");
        /*Log.d("InsertMethod", key);
        Log.d("VALD", successor);
        Log.d("VALD", predecessor);
        Log.d("VALD", myPort);*/

        FileOutputStream outputStream;

        value = values.getAsString("value");
        String hashed_key = null;


        try {

            hashed_key = genHash(key);
            Log.d("InsertingKeys", hashed_key + ":" + key);
            // Log.d(TAG,"Step_0"+key);
            if (myPort.equals(successor) && myPort.equals(predecessor)) {
                Log.d("Insert", "Single AVD");
                //only single avd
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);

                outputStream.write(value.getBytes());
                outputStream.close();
            } else if (hashed_key.compareTo(pred_hash) > 0 && hashed_key.compareTo(hashed_port) < 0) {
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                Log.d("InsertingFinally", key);

                outputStream.write(value.getBytes());
                outputStream.close();
            }

            //else if((hashed_port.compareTo(pred_hash)<0 && hashed_port.compareTo(succ_hash)<0)||(hashed_key.compareTo(hashed_port) < 0))
            else if (isFirstNode() && (hashed_key.compareTo(pred_hash) > 0 || hashed_key.compareTo(hashed_port) < 0))
            //else if (((genHash(portStr).compareTo(genHash(predecessor))==0)&&(genHash(portStr).compareTo(genHash(successor))==0))&&((hashed_key.compareTo(genHash(predecessor))>0)||hashed_key.compareTo(genHash(portStr))<0))
            {
                Log.d("InsertingFirstNode", key);
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);

                outputStream.write(value.getBytes());
                outputStream.close();
            } else {
                String msg_to_forward = portStr + "#" + type_of_work[2] + "#" + key + "#" + value;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_forward);
            }


        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean isFirstNode(){
        if(pred_hash.compareTo(hashed_port)>0 && succ_hash.compareTo(hashed_port)>0)
            return true;
        return false;
    }

    @Override
    public boolean onCreate() {
        // TODO OnCreate

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        String message = "";
        hashed_port = "";
        Log.d(TAG, "inside on create");
        Log.d(TAG, portStr);

        predecessor = myPort;
        successor = myPort;

        message = portStr + "#" + type_of_work[0] + "#" + predecessor + "#" + successor + "#" + key + "#" + value;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            hashed_port = genHash(portStr);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (!portStr.equals("5554"))
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
        else {
            port_and_hash.put(hashed_port, portStr);
            node_id.add(hashed_port);
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {


        String value_msg = " ";
        String selection_hashed = " ";
        Log.d("InsertReceived",selection);
        String msg_split[] = selection.split("#");
        int n = msg_split.length;
        Log.d("MessageSplitLength",Integer.toString(n));
        if(n>3){
            Log.d("MessageAtSplit3",msg_split[3]);
        }
        try {
            selection_hashed = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        // value to come from file
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});

        System.out.println("selection" + selection);

        if (myPort.equals(predecessor) && myPort.equals(successor)) {
            if (selection.equals("*") || selection.equals("@")) {

                String files[] = getContext().fileList();

                for (String list : files) {
                    try {
                        FileInputStream fin = getContext().openFileInput(list);
                        System.out.println("key:" + list);
                        InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
                        BufferedReader br = new BufferedReader(isr);
                        value_msg = br.readLine();
                        System.out.println("Value:" + value_msg);
                        cursor.addRow(new Object[]{list, value_msg});
                        fin.close();
                        isr.close();
                        br.close();
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    //Log.d(TAG, "what" + String.valueOf(cursor));

                }
                return cursor;
            } else {
                try {
                    //Log.d(TAG, "inside else");

                    FileInputStream fin = getContext().openFileInput(selection);
                    InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
                    BufferedReader br = new BufferedReader(isr);
                    value_msg = br.readLine();
                    fin.close();
                    isr.close();
                    br.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (RuntimeException e) {
                    e.printStackTrace();
                }
                cursor.addRow(new Object[]{selection, value_msg});
                //Log.d(TAG, "Message_final" + cursor);
                return cursor;
            }
        } else if (selection.equals("@")) {
            String files[] = getContext().fileList();
            for (String list : files) {
                try {
                    FileInputStream fin = getContext().openFileInput(list);
                    System.out.println("key:" + list);
                    InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
                    BufferedReader br = new BufferedReader(isr);
                    value_msg = br.readLine();
                    Log.d("Query condition @", selection);
                    Log.d("Value:", value_msg);
                    cursor.addRow(new Object[]{list, value_msg});
                    fin.close();
                    isr.close();
                    br.close();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                //Log.d(TAG, "what" + String.valueOf(cursor));

            }
            return cursor;
        } else if (selection.equals("*")) {
            String files[] = getContext().fileList();

            try {
                for (String list : files) {

                    FileInputStream fin = getContext().openFileInput(list);
                    //System.out.println("key:" + list);
                    InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
                    BufferedReader br = new BufferedReader(isr);
                    value_msg = br.readLine();
                    //Log.d("Query condition @ 4 *", selection);
                    //Log.d("Value:", value_msg);
                    cursor.addRow(new Object[]{list, value_msg});
                }
                //fin.close();
                //isr.close();
                //br.close();
                String originating_port = portStr;
                //while (!successor.equals(portStr)) {
                String msg_for_star = originating_port + "#" + type_of_work[8] + "#" + successor + "#" + "*";
                if (!originating_port.equals(successor)) {
                    String all_data = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_for_star).get();
                    String temp[] = all_data.split("#");
                    String keys[] = temp[2].split(",");
                    String values[] = temp[3].split(",");
                    for (int i = 0; i < keys.length; i++) {
                        cursor.addRow(new Object[]{keys[i], values[i]});

                    }
                }
                return cursor;


                // }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        else if (n>3) {
            String files[] = getContext().fileList();

            try {
                for (String list : files) {

                    FileInputStream fin = getContext().openFileInput(list);
                    System.out.println("key:" + list);
                    InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
                    BufferedReader br = new BufferedReader(isr);
                    value_msg = br.readLine();
                    Log.d("Query condition @ 4 *", selection);
                    Log.d("Value:", value_msg);
                    cursor.addRow(new Object[]{list, value_msg});
                }
                //fin.close();
                //isr.close();
                //br.close();
                String originating_port = msg_split[0];
                //while (!successor.equals(portStr)) {
                String msg_for_star = originating_port + "#" + type_of_work[8] + "#" + successor + "#" + "*";
                if (!originating_port.equals(successor)) {
                    String all_data = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_for_star).get();
                    String temp[] = all_data.split("#");
                    String keys[] = temp[2].split(",");
                    String values[] = temp[3].split(",");
                    for (int i = 0; i < keys.length; i++) {
                        cursor.addRow(new Object[]{keys[i], values[i]});

                    }
                }
                return cursor;



            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else if ((selection_hashed.compareTo(pred_hash) > 0) && selection_hashed.compareTo(hashed_port) < 0) {

            try {

                FileInputStream fin = getContext().openFileInput(selection);
                InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
                BufferedReader br = new BufferedReader(isr);
                value_msg = br.readLine();
                Log.d("Query condition @", hashed_port);
                Log.d("Value:", value_msg);
                fin.close();
                isr.close();
                br.close();

            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //Log.d(TAG, "what" + String.valueOf(cursor));
            cursor.addRow(new Object[]{selection, value_msg});
            return cursor;
        } else if (isFirstNode() && (selection_hashed.compareTo(pred_hash) > 0 || selection_hashed.compareTo(hashed_port) < 0)) {
            try {

                FileInputStream fin = getContext().openFileInput(selection);
                InputStreamReader isr = new InputStreamReader(fin, "UTF-8");
                BufferedReader br = new BufferedReader(isr);
                value_msg = br.readLine();
                Log.d("Query condition @", hashed_port);
                Log.d("Value:", value_msg);
                fin.close();
                isr.close();
                br.close();

            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //Log.d(TAG, "what" + String.valueOf(cursor));
            cursor.addRow(new Object[]{selection, value_msg});
            return cursor;
        } else {
            String msg_to_foward = portStr + "#" + type_of_work[6] + "#" + successor + "#" + selection;
            try {

                Log.d("Query condition forward", "Successor finder" + msg_to_foward);
                //Log.d("Value:" ,value_msg);
                String s = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg_to_foward).get();

                Log.d("Message received", s);
                String temp[] = s.split("#");
                cursor.addRow(new Object[]{temp[3], temp[4]});
                return cursor;


            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    // TODO Auto-generated method stub


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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
            String messages = null;
            Log.d(TAG, "I am here");
            try {
                while (true) {

                    Socket socket = serverSocket.accept();

                    InputStream in = socket.getInputStream();
                    DataInputStream data = new DataInputStream(in);
                    messages = data.readUTF();
                    OutputStream out = socket.getOutputStream();
                    DataOutputStream dout = new DataOutputStream(out);
                    //dout.writeUTF("OK");
                    //socket.close();

                    Log.d(TAG, "inside server task");

                    String msg[] = messages.split("#");

                    Log.d("ServerReceived", messages);
                    int index_current = 0;

                    String hashed_port = genHash(msg[0]);


                    if (msg[1].equals(type_of_work[1])) {

                        Log.d(TAG, "join started");
                        node_id.add(hashed_port);
                        Collections.sort(node_id);
                        port_and_hash.put(hashed_port, msg[0]);

                        Log.d(TAG, hashed_port + port_and_hash.get(hashed_port));
                        Log.d(TAG, "size of hash map" + port_and_hash.size());

                        Log.d(TAG, "size" + node_id.size());
                        for (int j = 0; j < node_id.size(); j++) {
                            Log.d(TAG, "elements" + node_id.get(j));

                        }

                        index_current = node_id.indexOf(hashed_port);
                        String localPred = "", localSucc = "";

                        if (index_current > 0 && index_current < node_id.size() - 1) {
                            localPred = port_and_hash.get(node_id.get(index_current - 1));
                            localSucc = port_and_hash.get(node_id.get(index_current + 1));
                            Log.d(TAG, "Successor" + localSucc);
                            Log.d(TAG, "Predeccesor" + localPred);
                        } else if (index_current == 0) {
                            localPred = port_and_hash.get(node_id.get(node_id.size() - 1));
                            localSucc = port_and_hash.get(node_id.get(1));
                            Log.d(TAG, "Successor" + localSucc);
                            Log.d(TAG, "Predeccesor" + localPred);
                        } else if (index_current == (node_id.size() - 1)) {
                            localPred = port_and_hash.get(node_id.get(index_current - 1));
                            localSucc = port_and_hash.get(node_id.get(0));
                            Log.d(TAG, "Successor" + localSucc);
                            Log.d(TAG, "Predeccesor" + localPred);
                        }

                        String msg_after_join = msg[0] + "#" + type_of_work[3] + "#" + localPred + "#" + localSucc + "#" + key + "#" + value;

                        Log.d(TAG, "the msg for publishing ");
                        System.out.print("msg after join" + msg_after_join);
                        dout.writeUTF("OK");
                        publishProgress(msg_after_join);


                    } else if (msg[1].equals(type_of_work[2])) {

                        ContentValues value = new ContentValues();
                        value.put("key", msg[2]);
                        value.put("value", msg[3]);
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri uri = uriBuilder.build();
                        insert(uri, value);
                        dout.writeUTF("OK");

                    } else if (msg[1].equals(type_of_work[3])) {

                        predecessor = msg[2];
                        successor = msg[3];

                        try {
                            succ_hash = genHash(successor);
                            pred_hash = genHash(predecessor);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        dout.writeUTF("OK");
                    } else if (msg[1].equals(type_of_work[4])) {
                        successor = msg[3];
                        try {
                            succ_hash = genHash(successor);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        dout.writeUTF("OK");
                    } else if (msg[1].equals(type_of_work[5])) {
                        predecessor = msg[2];
                        try {
                            pred_hash = genHash(predecessor);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                        dout.writeUTF("OK");
                    } else if (msg[1].equals(type_of_work[6])) {
                        //calling query

                        Log.d("forward condition", msg[3]);
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri uri = uriBuilder.build();
                        Cursor query_search = query(uri, null, msg[3], null, null);
                        //query_search.moveToFirst();

                        int keyIndex = query_search.getColumnIndex("key");
                        int valueIndex = query_search.getColumnIndex("value");

                        Log.d("Sending the query", String.valueOf(keyIndex));
                        Log.d("Sending the query", String.valueOf(valueIndex));

                        query_search.moveToFirst();

                        String query_search_key = query_search.getString(keyIndex);
                        String query_search_value = query_search.getString(valueIndex);

                        Log.d("Sending the query", value);
                        query_search.close();

                        String msg_after_selection = portStr + "#" + type_of_work[7] + "#" + successor + "#" + query_search_key + "#" + query_search_value;
                        Log.d("final message", msg_after_selection);
                        dout.writeUTF(msg_after_selection);
                        //query_search.close();
                        //socket.close();

                        //publishProgress(msg_after_selection);

                    } else if (msg[1].equals(type_of_work[8]))

                    {
                        Log.d("QueryReceived",messages);
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri uri = uriBuilder.build();
                        Cursor query_search_for_each_port = query(uri, null, messages, null, null);
                        String keys="";
                        String values="";
                        while(query_search_for_each_port.moveToNext()){
                            keys = keys + query_search_for_each_port.getString(query_search_for_each_port.getColumnIndex("key")) + ",";
                            values = values + query_search_for_each_port.getString(query_search_for_each_port.getColumnIndex("value")) + ",";
                        }

                        Log.d("KeysBefore",keys);
                        Log.d("ValuesBefore",values);

                        //keys = keys.substring(0,keys.length()-1);
                        //values = values.substring(0,values.length()-1);

                        Log.d("KeysAfter",keys);
                        Log.d("ValuesAfter",values);

                        String msg_final=msg[0]+"#"+msg[1]+"#"+keys+"#"+values;
                        dout.writeUTF(msg_final);


                        Log.d("Sending the query", value);
                        query_search_for_each_port.close();

                    }

                   /* else if (msg[1].equals(type_of_work[9]))
                    {
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri uri = uriBuilder.build();
                    }*/

                   else if (msg[1].equals(type_of_work[9])) {
                        Uri.Builder uriBuilder = new Uri.Builder();
                        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
                        uriBuilder.scheme("content");
                        Uri uri = uriBuilder.build();
                        Log.d("Deletion Server",msg[3]);
                        delete(uri,msg[3],null);
                        //dout.write("OK");
                    }

                    socket.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {


            String split_msg[] = strings[0].split("#");
            if (split_msg[1].equals(type_of_work[3])) {
                Log.d("SendingNewInfo", "JOIN JOIN JOIN");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0]);

                String new_suc_msg = split_msg[2] + "#" + type_of_work[4] + "#" + "XX" + "#" + split_msg[0];
                String new_pred_msg = split_msg[3] + "#" + type_of_work[5] + "#" + split_msg[0];
                Log.d("NewSuccessorAsWell", new_suc_msg);
                Log.d("NewPredecessorAsWell", new_pred_msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new_suc_msg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new_pred_msg);

            }


        }


    }

    private class ClientTask extends AsyncTask<String, String, String> {

        protected String doInBackground(String... msgs) {

            String message[] = msgs[0].split("#");
            System.out.print("#############");
            int index_current = 0;
            //System.out.print("Check time"+port);
            Log.d(TAG, "I am out");

            try {

                if (message[1].equals(type_of_work[0])) {
                    System.out.print("request for join to avd 5554");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));

                    System.out.print("resssuullttt");

                    String incoming_port_value = genHash(message[0]);

                    //Log.d(TAG, "message before join" + message_for_join[1]);
                    String msg_join = message[0] + "#" + type_of_work[1] + "#" + predecessor + "#" + successor + "#" + key + "#" + value;

                    OutputStream out = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF(msg_join);
                    String ack;

                    InputStream in = socket.getInputStream();
                    DataInputStream din = new DataInputStream(in);

                    ack = din.readUTF();

                    if (ack.equals("OK"))
                        socket.close();

                    out.close();
                    d.close();
                    in.close();
                    din.close();

                    System.out.print(msg_join);
                    System.out.print(node_id.size());
                } else if (message[1].equals(type_of_work[3])) {
                    Log.d(TAG, "sending successor pred info");

                    int my_port = Integer.parseInt(message[0]) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            my_port);

                    String msg_pre_suc = msgs[0];
                    Log.v(TAG, "ClientSocket" + message[0]);

                    OutputStream out = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF(msg_pre_suc);

                    String ack;

                    InputStream in = socket.getInputStream();
                    DataInputStream din = new DataInputStream(in);

                    ack = din.readUTF();

                    if (ack.equals("OK"))
                        socket.close();

                    out.close();
                    d.close();
                    in.close();
                    din.close();


                } else if (message[1].equals(type_of_work[2])) {
                    Log.d(TAG, "sending to successor");


                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successor) * 2);

                    String msg_to_insert = msgs[0];
                    Log.v(TAG, "ClientSocket" + message[3]);

                    OutputStream out = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF(msg_to_insert);

                    String ack;

                    InputStream in = socket.getInputStream();
                    DataInputStream din = new DataInputStream(in);

                    ack = din.readUTF();

                    if (ack.equals("OK"))
                        socket.close();

                    out.close();
                    d.close();
                    in.close();
                    din.close();


                } else if (message[1].equals(type_of_work[4]) || message[1].equals(type_of_work[5])) {

                    int port = Integer.parseInt(message[0]) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port);

                    String msg_to_insert = msgs[0];
                    Log.v(TAG, "SendingNewSuccessorOrPred" + msg_to_insert + "DDD" + message[0]);

                    OutputStream out = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF(msg_to_insert);

                    String ack;

                    InputStream in = socket.getInputStream();
                    DataInputStream din = new DataInputStream(in);

                    ack = din.readUTF();

                    if (ack.equals("OK"))
                        socket.close();

                    out.close();
                    d.close();
                    in.close();
                    din.close();


                } else if (message[1].equals(type_of_work[6])) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(message[2]) * 2);

                    String msg_to_insert = msgs[0];
                    Log.v(TAG, "SendingForwardRequest" + msg_to_insert + "FFF" + message[0]);

                    OutputStream out = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF(msg_to_insert);

                    String ack;
                    //String msg_to_query;

                    InputStream in = socket.getInputStream();
                    DataInputStream din = new DataInputStream(in);

                    ack = din.readUTF();


                    Log.d("Replying back", ack);

                    if (ack.equals("OK"))
                        socket.close();
                    else {
                        socket.close();
                        return ack;

                    }

                    out.close();
                    d.close();
                    in.close();
                    din.close();


                } else if (message[1].equals(type_of_work[8])) {

                    int port = Integer.parseInt(message[2]) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port);

                    String msg_to_insert = msgs[0];
                    Log.v(TAG, "AllDataRetrieved" + msg_to_insert + "RRR");

                    OutputStream out = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF(msg_to_insert);

                    String ack;

                    InputStream in = socket.getInputStream();
                    DataInputStream din = new DataInputStream(in);

                    ack = din.readUTF();

                    if (ack.equals("OK"))
                        socket.close();
                    else {
                        socket.close();
                        return ack;
                    }

                    out.close();
                    d.close();
                    in.close();
                    din.close();


                }

               /* else if(message[1].equals(type_of_work[9]))
                {
                    int port = Integer.parseInt(message[2]) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port);
                    String msg_to_delete= msgs[0];
                    Log.v(TAG, "SendingForwardDelete star" + msg_to_delete + "FFF");

                    OutputStream out = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF(msg_to_delete);
                    socket.close();
                    out.close();
                    d.close();

                }*/

                else if (message[1].equals(type_of_work[9]))
                {
                    int port = Integer.parseInt(message[2]) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port);
                    String msg_to_delete= msgs[0];

                    Log.v(TAG, "SendingForwardDelete" + msg_to_delete + "FFF");

                    OutputStream out = socket.getOutputStream();
                    DataOutputStream d = new DataOutputStream(out);
                    d.writeUTF(msg_to_delete);

                   //String ack;
                    //String msg_to_query;

                    //InputStream in = socket.getInputStream();
                    //DataInputStream din = new DataInputStream(in);

                    //ack = din.readUTF();


                    //Log.d("Replying back", ack);

                    //if (ack.equals("OK"))
                       // socket.close();
                    //else {
                        socket.close();
                      //  return ack;

                    //}

                    out.close();
                    d.close();
                    //in.close();
                    //din.close();

                }

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}
