package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Hashtable;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static private final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final String[] REMOTE_PORT = {"11124","11112","11108","11116","11120"};
    static final String[] Node_List= {"5562","5556","5554","5558","5560"};
    static final int SERVER_PORT = 10000;
    static final String msgDelimiter="####";
    static final String cursorDelimiter="<==>";

    //variable to Identify the incoming msg in Server
    static final String InsertTask="InsertTask";
    static final String InsertReplicateTask="InsertReplicateTask";
    static final String QueryTask="QueryTask";
    static final String ReturnQueryTask="ReturnQueryTask";
    static final String DeleteTask="DeleteTask";
    static final String StarQuery="StarQuery";
    static final String StarDelete="StarDelete";
    static final String ReturnStarQuery="ReturnStarQuery";
    static final String RecoveryTask = "RecoveryTask";
    static final String UpdateTask= "UpdateTask";

//    static boolean isQueryOriginatingPort=true;
//    static String queryOriginatingPort="";
    //uri config
    final static Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    static final String allMsgs= "\"@\"";
    static final String allDHTMsgs="\"*\"";

    static final String[] columnNames = {KEY_FIELD, VALUE_FIELD};
//    static MatrixCursor allMsgCursor = new MatrixCursor(columnNames);
    static Hashtable<String,String> queryTable = new Hashtable<>();
    static MatrixCursor starMsgCursor = new MatrixCursor(columnNames);
    static final String NULL="null";

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        if(selection.equals(allMsgs)){
            String[] files=getContext().fileList();
            for(String file: files){
                getContext().deleteFile(file);
            }
        }else if(selection.equals(allDHTMsgs)){
            String[] files=getContext().fileList();
            for(String file: files){
                getContext().deleteFile(file);
            }
            new DeleteStarQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//
//            }
        }else {
//            ContextVariables contextVariables= (ContextVariables)getContext();
//            String destNode = keyLookUp(selection);
//            if(destNode.equals(contextVariables.getMyNodeId())){
//                Log.w(TAG,"Deleting in self - key : " + selection);
                getContext().deleteFile(selection);
//            }else{
////                Log.w(TAG,"Insert Task to"+contextVariables.getMySuccessor()+" for key : "+key+" with hash : "+genHash(key));
//                new DeleteClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,destNode,selection);
//            }
//            new DeleteReplicaTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,destNode,selection);
        }
        return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        String key;
        String value;
        ContextVariables contextVariables= (ContextVariables)getContext();
        try {
            key=values.get(KEY_FIELD).toString();
            value= values.get(VALUE_FIELD).toString();
            Log.i("--------------------------","inserting "+key+"-"+value);
            String destNode = keyLookUp(key);
            if(destNode.equals(contextVariables.getMyNodeId())){
//                Log.w(TAG,"Inserting key @"+contextVariables.getMyNodeId()+" : "+key+" with hash : "+genHash(key));
                FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                fos.write(value.getBytes());
                fos.close();
            }else{
//                Log.w(TAG,"Insert Task to"+contextVariables.getMySuccessor()+" for key : "+key+" with hash : "+genHash(key));
                new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,getPortId(destNode),key,value);
            }
            new InsertReplicateClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,getPortId(destNode),key,value);
        }catch(IOException ioe){
            Log.e(TAG,"Insert Exception"+ioe.getMessage());
        }catch(NullPointerException npe){
            Log.e(TAG,"Insert Exception"+npe.getMessage());
        }catch(Exception e){
            Log.e(TAG,"Insert Exception"+e.getMessage());
        }
        Log.v("insert", values.toString());
        return uri;
	}

//    public synchronized Uri replicateInsert(Uri uri, ContentValues values) {
//        String key;
//        String value;
//        ContextVariables contextVariables= (ContextVariables)getContext();
//        try {
//            key=values.get(KEY_FIELD).toString();
//            value= values.get(VALUE_FIELD).toString();
////                Log.w(TAG,"Inserting key @"+contextVariables.getMyNodeId()+" : "+key+" with hash : "+genHash(key));
//            FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
//            fos.write(value.getBytes());
//            fos.close();
//        }catch(IOException ioe){
//            Log.e(TAG,"Insert Exception"+ioe.getMessage());
//        }catch(NullPointerException npe){
//            Log.e(TAG,"Insert Exception"+npe.getMessage());
//        }catch(Exception e){
//            Log.e(TAG,"Insert Exception"+e.getMessage());
//        }
//        Log.v("insert", values.toString());
//        return uri;
//    }

	@Override
	public boolean onCreate() {
        try{
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            ContextVariables contextVariables = (ContextVariables) getContext();
            contextVariables.setMyPort(myPort);
            contextVariables.setMyNodeId(portStr);
            contextVariables.setMyHashNode(genHash(portStr));
            int index= Arrays.asList(Node_List).indexOf(portStr);
            contextVariables.setMySuccessorOne(Node_List[(index+1)%5]);
            contextVariables.setMySuccessorTwo(Node_List[(index+2)%5]);
            Log.w(TAG,"OnCreate of "+contextVariables.getMyNodeId()+" with preferenceList "+contextVariables.getMySuccessorOne()+" ,"+contextVariables.getMySuccessorTwo());
            //Initiating the server task
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, "IOException Can't create a ServerSocket");
                return false;
            } catch (Exception e){
                Log.e(TAG, "Exception can't create a ServerSocket");
                return false;
            }
            //String[] files=getContext().fileList();

            String[] files=getContext().fileList();
            if(files!=null && files.length>0){
                // this indicates the node is recovering after a failure
                // hence send recovered msg to other nodes.
                for(String file: files){
                    getContext().deleteFile(file);
                }
                new RecoveryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portStr);
            }
        }catch(Exception e){
        Log.e(TAG," onCreate Exception : " + e.getMessage());
    }
		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        Log.i("--------------------------","querying for "+selection);
        String msgValue="" ;
        MatrixCursor msgCursor = new MatrixCursor(columnNames);
        int keyIndex = msgCursor.getColumnIndex(KEY_FIELD);
        int valueIndex = msgCursor.getColumnIndex(VALUE_FIELD);

        FileInputStream fis;
        BufferedInputStream bis;
        int temp;
        ContextVariables contextVariables= (ContextVariables)getContext();
        if(selection.equals(allMsgs)){
            String[] files=getContext().fileList();
            for(String file: files){
                msgValue="";
                try {
                    fis = getContext().openFileInput(file);
                    bis = new BufferedInputStream(fis);
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (IOException ioe) {

                } catch (NullPointerException npe) {

                }
                msgCursor.addRow(new String[]{file, msgValue});
            }
            return msgCursor;
        }else if(selection.equals(allDHTMsgs)){

            String[] files=getContext().fileList();
            for(String file: files){
                msgValue="";
                try {
                    fis = getContext().openFileInput(file);
                    bis = new BufferedInputStream(fis);
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                } catch (IOException ioe) {
                    Log.w(TAG,"STar Cursor processing exception"+ ioe.getMessage());
                } catch (NullPointerException npe) {
                    Log.w(TAG,"STar Cursor processing exception"+ npe.getMessage());
                }
                starMsgCursor.addRow(new String[]{file, msgValue});
            }
//            if(!contextVariables.getMyNodeId().equals(contextVariables.getMySuccessor())) {
                new StarQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, contextVariables.getMyPort());
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {

                }


//            }
            return starMsgCursor;
        }
        else {
            try {
//                allMsgCursor = new MatrixCursor(columnNames);
                queryTable.put(selection,NULL);
                String destNode = keyLookUp(selection);
//                Log.w(TAG, "Query to be performed on key :"+selection);
                if(destNode.equals(contextVariables.getMyNodeId())){
                    fis = getContext().openFileInput(selection);
                    bis = new BufferedInputStream(fis);
                    while ((temp = bis.read()) != -1) {
                        msgValue += (char) temp;
                    }
                    bis.close();
                    Log.w(TAG,"Query processed successfully for key :"+selection+" with value :"+msgValue);
                    msgCursor.addRow(new String[]{selection, msgValue});
//                    return msgCursor;
                }else{
                    Log.w(TAG, "Query Task to be passed to "+destNode+" for key :"+selection);
                    //IF ORIGINATING PORT IS NOT CLEARLY PASSED INSTEAD NEW PORT IS BEING PASSED
                    String originatingPort=contextVariables.getMyPort();
//                    if(!isQueryOriginatingPort){
//                        originatingPort=queryOriginatingPort;
//                    }
                    new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, getPortId(destNode), selection, originatingPort);
////                    boolean queryReturned =false;
////                    do {
//                    if(isQueryOriginatingPort) {
//                        Thread.sleep(1500);
////                        Log.w(TAG, "Thread sleep end");
//                    }
                    while(true){
                        if(queryTable.get(selection)!=null && !queryTable.get(selection).equals(NULL)){
                            break;
                        }
//                        if(allMsgCursor.getCount()!=0){
//                            break;
//                        }
                    }

//                    if (allMsgCursor.moveToFirst()) {
//                        do {
//                            if(selection.equals(allMsgCursor.getString(keyIndex))){
//                                msgCursor.addRow(new String[]{selection, allMsgCursor.getString(valueIndex)});
//                                return msgCursor;
//                            }
//                        } while (allMsgCursor.moveToNext());
//                    }
                    if(queryTable.get(selection)!=null && !queryTable.get(selection).equals(NULL)) {
                        msgCursor.addRow(new String[]{selection, queryTable.get(selection)});
                        return msgCursor;
                    }else{
                        Log.w(TAG,"Cursor is empty");
                    }
                }
            } catch (IOException ioe) {
                Log.w(TAG," Query IOexception"+ ioe.getMessage());
            } catch (NullPointerException npe) {
                Log.w(TAG," Query NullPointerException"+ npe.getMessage());
//            } catch (InterruptedException e) {
//                Log.w(TAG," Query InterruptedException"+ e.getMessage());
            } catch (Exception e){
                Log.w(TAG," Query Exception"+ e.getMessage());
            }

        }
//        Log.v("query", selection);
        return msgCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            try {
                while(true) {
                    Socket socket = serverSocket.accept();//socket that connects to individual clients
                    InputStream is = socket.getInputStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                    String strIncomingMsg = br.readLine();
                    socket.close(); //closing the socket once the msg is read
                    Log.i("--------------------------",strIncomingMsg);
                    String[] msgs = strIncomingMsg.split(msgDelimiter);
                    ContextVariables contextVariables = (ContextVariables) getContext();
                    ContentValues keyValueToInsert;
                    String msgValue="";
                    String key;
                    String value;
                    FileInputStream fis;
                    BufferedInputStream bis;
                    int temp;
                    switch (msgs[0]) {
                        case RecoveryTask:
                            //RecoveryTask####originatingNode
                            Log.w(TAG,"received msg for recovery at "+contextVariables.getMyNodeId() +" for key"+msgs[1]);
                            String[] files=getContext().fileList();
//                            int index= Arrays.asList(Node_List).indexOf(msgs[1]);
//                            String successorOne = Node_List[(index+1)%5];
//                            String successorTwo = Node_List[(index+2)%5];
                            String predecessorOne = getPredecessorOne(msgs[1]);//Node_List[((index-1)%5 >= 0)?(index-1)%5:((index-1)%5) + 5];
                            String predecessorTwo = getPredecessorTwo(msgs[1]);//Node_List[((index-2)%5 >= 0)?(index-2)%5:((index-2)%5) + 5];
                            String updateMsg="";
                            for(String file: files){
                                String coordinator=keyLookUp(file);
                                if(coordinator.equals(msgs[1]) ||coordinator.equals(predecessorOne) ||coordinator.equals(predecessorTwo)) {
                                    msgValue = "";
                                    try {
                                        fis = getContext().openFileInput(file);
                                        bis = new BufferedInputStream(fis);
                                        while ((temp = bis.read()) != -1) {
                                            msgValue += (char) temp;
                                        }
                                        bis.close();
                                    } catch (IOException ioe) {
                                        Log.e(TAG,"IOException processing requestTask : " + ioe.getMessage());
                                    } catch (NullPointerException npe) {
                                        Log.e(TAG,"NullPointerException processing requestTask : " + npe.getMessage());
                                    }
                                    updateMsg+=file+cursorDelimiter+msgValue+msgDelimiter;
                                }
                            }
                            new UpdateClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[1], updateMsg);

                            break;
                        case UpdateTask:
                            //UpdateTask####Key1<==>Value1#####....keyn<==>valuen####
                            Log.w(TAG,"received Updatemsg : "+strIncomingMsg);
                            for(String msg: msgs) {
                                if(msg.contains(cursorDelimiter)) {
                                    key = msg.split(cursorDelimiter)[0];
                                    value = msg.split(cursorDelimiter)[1];
                                    FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                                    fos.write(value.getBytes());
                                    fos.close();
                                }
                            }
                            break;
                        case InsertTask:
                            //InsertTask####key####value####
                            Log.w(TAG,"received msg to insert at "+contextVariables.getMyNodeId() +" for key"+msgs[1]);
//                            keyValueToInsert = new ContentValues();
//                            keyValueToInsert.put(KEY_FIELD, msgs[1]);
//                            keyValueToInsert.put(VALUE_FIELD, msgs[2]);
//                            insert(uri, keyValueToInsert);
                            try {
                                key=msgs[1];
                                value= msgs[2];
                                FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                                fos.write(value.getBytes());
                                fos.close();
                            }catch(IOException ioe){
                                Log.e(TAG,"InsertTask Exception"+ioe.getMessage());
                            }catch(NullPointerException npe){
                                Log.e(TAG,"InsertTask Exception"+npe.getMessage());
                            }catch(Exception e){
                                Log.e(TAG,"InsertTask Exception"+e.getMessage());
                            }
                            break;
                        case InsertReplicateTask:
                            Log.w(TAG, "received msg to insert replicate at " + contextVariables.getMyNodeId() + " for key" + msgs[1]);
//                            keyValueToInsert = new ContentValues();
//                            keyValueToInsert.put(KEY_FIELD, msgs[1]);
//                            keyValueToInsert.put(VALUE_FIELD, msgs[2]);
//                            replicateInsert(uri, keyValueToInsert);
                            try {
                                key=msgs[1];
                                value= msgs[2];
                                FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                                fos.write(value.getBytes());
                                fos.close();
                            }catch(IOException ioe){
                                Log.e(TAG,"ReplicateInsert Exception"+ioe.getMessage());
                            }catch(NullPointerException npe){
                                Log.e(TAG,"ReplicateInsert Exception"+npe.getMessage());
                            }catch(Exception e){
                                Log.e(TAG,"ReplicateInsert Exception"+e.getMessage());
                            }
                            break;
                        case QueryTask:
                            //QueryTask####key####originatingPort####
//                            isQueryOriginatingPort = false;
//                            queryOriginatingPort = msgs[2];
                            Log.w(TAG, "Query request @ " + contextVariables.getMyNodeId() + " for key" + msgs[1] + " from " + msgs[2]);
                            MatrixCursor resultCursor = new MatrixCursor(columnNames);//= query(uri, null, msgs[1], null, null);

//                            int temp;
//                            String msgValue="" ;
                            try {
                                fis = getContext().openFileInput(msgs[1]);
                                bis = new BufferedInputStream(fis);
                                while ((temp = bis.read()) != -1) {
                                    msgValue += (char) temp;
                                }
                                bis.close();
                                Log.w(TAG,"Query processed successfully for key :"+msgs[1]+" with value :"+msgValue);
                                resultCursor.addRow(new String[]{msgs[1], msgValue});
                            }catch(IOException ioe){
                                Log.e(TAG,"QueryTask Exception"+ioe.getMessage());
                            }catch(NullPointerException npe){
                                Log.e(TAG,"QueryTask Exception"+npe.getMessage());
                            }catch(Exception e){
                                Log.e(TAG,"QueryTask Exception"+e.getMessage());
                            }



                            if (resultCursor.moveToFirst()) {
                                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                                key = resultCursor.getString(keyIndex);
                                value = resultCursor.getString(valueIndex);
                                String msgToSend = ReturnQueryTask + msgDelimiter + key + msgDelimiter + value + msgDelimiter + msgs[2] + msgDelimiter;//ReturnQueryTask####key####value####originatingPort####
                                callClientTask(msgToSend);
                            }else{
                                Log.w(TAG, "Empty Query Result @ " + contextVariables.getMyNodeId() + " for key" + msgs[1] + " from " + msgs[2]);
                            }
//                            isQueryOriginatingPort = true;
//                            queryOriginatingPort = "";
                            break;
                        case ReturnQueryTask:
                            //ReturnQueryTask####key####value####
                            Log.w(TAG,"Return Query Task from "+ msgs[3]+ " for key " + msgs[1]);
//                            allMsgCursor.addRow(new String[]{msgs[1], msgs[2]});
                            queryTable.put(msgs[1],msgs[2]);
                            break;
                        case StarQuery:
                            //StarQuery####originatingPort####
                            String rowList = "";
                            if (!msgs[1].equals(contextVariables.getMyPort())) {

                                Cursor resultStarCursor = query(uri, null, allMsgs, null, null);
                                if (resultStarCursor != null && resultStarCursor.getCount() > 0) {
                                    int keyIndex = resultStarCursor.getColumnIndex(KEY_FIELD);
                                    int valueIndex = resultStarCursor.getColumnIndex(VALUE_FIELD);
                                    if (resultStarCursor.moveToFirst()) {
                                        do {
                                            rowList += resultStarCursor.getString(keyIndex) + cursorDelimiter + resultStarCursor.getString(valueIndex) + msgDelimiter;
                                        } while (resultStarCursor.moveToNext());
                                    }
                                    new ReturnStarQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[1], rowList);

                                }
//                                new StarQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, getPortId(contextVariables.getMySuccessor()), msgs[1]);
                            }
                            break;
                        case ReturnStarQuery:
                            //ReturnStarQuery####key1<==>value1####...keyn<==>valuen####

                            for (String msg : msgs) {
                                if (msg.contains(cursorDelimiter)) {
                                    starMsgCursor.addRow(new String[]{msg.split(cursorDelimiter)[0], msg.split(cursorDelimiter)[1]});
                                }
                            }
//                            starMsgCursor.addRow(new String[]{file, msgValue});
                            break;
                        case StarDelete:
                                files=getContext().fileList();
                                for(String file: files){
                                    getContext().deleteFile(file);
                                }
                            break;
                        case DeleteTask:
                            //DeleteTask####selection####
                            Log.w(TAG,"Delete Task for key : " + msgs[1]);
                            getContext().deleteFile(msgs[1]);
                            break;
                    }
                }
            } catch (IOException e) {
                Log.e(TAG, " Server Task IOException :" + e.getMessage()+ " ==> ");
                e.printStackTrace();
            } catch (Exception e){
                Log.e(TAG, " Server Task Exception :" + e.getMessage());
                e.printStackTrace();
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */


            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */

        }

        protected void callClientTask(String msg){

            if(msg!=null) {
                String[] msgs = msg.split(msgDelimiter);
                switch (msgs[0]) {
                    case InsertTask:

                        break;
                    case ReturnQueryTask:
                        //ReturnQueryTask####key####value####originatingPort####
                        new ReturnQueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[3],msgs[1],msgs[2]);
                        break;
                    case DeleteTask:


                }
            }
        }


    }

    private class RecoveryClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            //msgs 0 - originatingNode
            ContextVariables contextVariables= (ContextVariables)getContext();
            Socket socket;
            OutputStream os;
            PrintWriter pw;

//            int index= Arrays.asList(REMOTE_PORT).indexOf(msgs[0]);
//            String[] preferenceList = new String[]{REMOTE_PORT[(index+1)%5],REMOTE_PORT[(index+2)%5]};
            for(String port: REMOTE_PORT){
                if(!port.equals(getPortId(msgs[0]))) {
                    try {
                        String msgToSend = RecoveryTask + msgDelimiter + msgs[0];//RecoveryTask####originatingNode
                        Log.w(TAG, "Recovery Client Task msg to " + port + " :" + msgToSend);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        os = socket.getOutputStream();
                        pw = new PrintWriter(os, true);
                        pw.println(msgToSend);
                        pw.flush();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Recovery ClientTask UnknownHostException for port "+ port+" : " + e.getMessage());
                    } catch (IOException e) {
                        Log.e(TAG, "Recovery ClientTask socket IOException "+ port+": " + e.getMessage());
                    } catch (Exception e) {
                        Log.e(TAG, "Recovery ClientTask Exception "+ port+": " + e.getMessage());
                    }
                }
            }

            return null;
        }
    }

    private class UpdateClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            //msgs 0 - originatingNode, msgs 1  = key+cursorDelimiter+value
            ContextVariables contextVariables= (ContextVariables)getContext();
            Socket socket;
            OutputStream os;
            PrintWriter pw;

//            int index= Arrays.asList(REMOTE_PORT).indexOf(msgs[0]);
//            String[] preferenceList = new String[]{REMOTE_PORT[(index+1)%5],REMOTE_PORT[(index+2)%5]};
//            for(String port: REMOTE_PORT){
//                if(port.equals(getPortId(msgs[0]))) {
            try {
                String msgToSend = UpdateTask + msgDelimiter + msgs[1];//UpdateTask####Key1<==>Value1#####....keyn<==>valuen####
                Log.w(TAG, "Update Client Task msg to " + getPortId(msgs[0]) + " :" + msgToSend);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(getPortId(msgs[0])));
                os = socket.getOutputStream();
                pw = new PrintWriter(os, true);
                pw.println(msgToSend);
                pw.flush();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "Update ClientTask UnknownHostException for port "+ getPortId(msgs[0])+" : " + e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Update ClientTask socket IOException "+ getPortId(msgs[0])+": " + e.getMessage());
            } catch (Exception e) {
                Log.e(TAG, "Update ClientTask Exception "+ getPortId(msgs[0])+": " + e.getMessage());
            }
//                }
//            }

            return null;
        }
    }

    private class InsertClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

                //msgs 0 - dest portId, 1 - key, 2 -value
                ContextVariables contextVariables= (ContextVariables)getContext();
                Socket socket;
                OutputStream os;
                PrintWriter pw;
                String msgToSend = InsertTask+msgDelimiter+msgs[1]+msgDelimiter+msgs[2]+msgDelimiter;//InsertTask####key####value####
                Log.w(TAG,"Insert Client Task msg to "+msgs[0]+" :"+msgToSend);
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[0]));
                    os= socket.getOutputStream();
                    pw= new PrintWriter(os,true);
                    pw.println(msgToSend);
                    pw.flush();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "Insert ClientTask UnknownHostException : "+e.getMessage());
                } catch (IOException e) {
                    Log.e(TAG, "Insert ClientTask socket IOException : "+e.getMessage());
                } catch (Exception e){
                    Log.e(TAG, " Insert ClientTAsk Exception :" + e.getMessage());
                }
            return null;
        }
    }

    private class InsertReplicateClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            //msgs 0 - dest portId, 1 - key, 2 -value
            ContextVariables contextVariables= (ContextVariables)getContext();
            Socket socket;
            OutputStream os;
            PrintWriter pw;

            int index= Arrays.asList(REMOTE_PORT).indexOf(msgs[0]);
            String[] preferenceList = new String[]{REMOTE_PORT[(index+1)%5],REMOTE_PORT[(index+2)%5]};
            for(String replicatePort: preferenceList){
                try {
                    String msgToSend = InsertReplicateTask + msgDelimiter + msgs[1] + msgDelimiter + msgs[2] + msgDelimiter;//InsertReplicateTask####key####value####
                    Log.w(TAG, "Insert Replicate Client Task msg to " + replicatePort + " :" + msgToSend);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(replicatePort));
                    os = socket.getOutputStream();
                    pw = new PrintWriter(os, true);
                    pw.println(msgToSend);
                    pw.flush();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "InsertReplicate ClientTask UnknownHostException : "+e.getMessage());
                } catch (IOException e) {
                    Log.e(TAG, "InsertReplicate ClientTask socket IOException : "+e.getMessage());
                } catch (Exception e){
                    Log.e(TAG, " InsertReplicate ClientTask Exception :" + e.getMessage());
                }
            }

            return null;
        }
    }

    private class QueryClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Socket socket;
            OutputStream os;
            PrintWriter pw;
            String msgToSend = QueryTask+msgDelimiter+msgs[1]+msgDelimiter+msgs[2]+msgDelimiter;//QueryTask####key####originatingPort####
            Log.w(TAG,"Query Client Task msg to "+msgs[0]+" :"+msgToSend);

            try {
                //msgs 0 - destnode, 1 - key, 2 - originatingPort
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[0]));
                os= socket.getOutputStream();
                pw= new PrintWriter(os,true);
                pw.println(msgToSend);
                pw.flush();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "Query ClientTask UnknownHostException : "+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Query ClientTask socket IOException : "+e.getMessage());
            } catch (Exception e){
                Log.e(TAG, " Query ClientTAsk Exception :" + e.getMessage());
            }
            int index= Arrays.asList(REMOTE_PORT).indexOf(msgs[0]);
            String[] preferenceList = new String[]{REMOTE_PORT[(index+1)%5],REMOTE_PORT[(index+2)%5]};
            for(String replicaPort: preferenceList){
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(replicaPort));
                    os = socket.getOutputStream();
                    pw = new PrintWriter(os, true);
                    pw.println(msgToSend);
                    pw.flush();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "Query ClientTask UnknownHostException : "+e.getMessage());
                } catch (IOException e) {
                    Log.e(TAG, "Query ClientTask socket IOException : "+e.getMessage());
                } catch (Exception e){
                    Log.e(TAG, " Query ClientTAsk Exception :" + e.getMessage());
                }
            }
            return null;
        }
    }

    private class ReturnQueryClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                ContextVariables contextVariables= (ContextVariables)getContext();
                //msgs 0 -  originatingPortId, 1 - key ,2 -value
                Socket socket;
                OutputStream os;
                PrintWriter pw;
                String msgToSend = ReturnQueryTask+msgDelimiter+msgs[1]+msgDelimiter+msgs[2]+msgDelimiter+contextVariables.getMyNodeId()+msgDelimiter;//ReturnQueryTask####key####value####sending portid####
                Log.w(TAG,"Return Query Client Task msg to "+msgs[0]+" :"+msgToSend);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[0]));
                os= socket.getOutputStream();
                pw= new PrintWriter(os,true);
                pw.println(msgToSend);
                pw.flush();
               socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "Query ClientTask UnknownHostException : "+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Query ClientTask socket IOException : "+e.getMessage());
            } catch (Exception e){
                Log.e(TAG, " Query ClientTAsk Exception :" + e.getMessage());
            }

            return null;
        }
    }

    private class StarQueryClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {

                //msgs 0 - originatingPort
                Socket socket;
                OutputStream os;
                PrintWriter pw;
                String msgToSend = StarQuery+msgDelimiter+msgs[0]+msgDelimiter;//StarQuery####originatingPort####
                for(String port :REMOTE_PORT){
                    if(port!=getPortId(msgs[0])){
                        Log.w(TAG,"Star Query Client Task msg to "+msgs[0]+" :"+msgToSend);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port));
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(msgToSend);
                        pw.flush();
                        socket.close();
                    }
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "Star Query ClientTask UnknownHostException : "+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Star Query ClientTask socket IOException : "+e.getMessage());
            } catch (Exception e){
                Log.e(TAG, "Star Query ClientTAsk Exception :" + e.getMessage());
            }

            return null;
        }
    }

    private class ReturnStarQueryClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                ContextVariables contextVariables= (ContextVariables)getContext();
                Socket socket;
                OutputStream os;
                PrintWriter pw;
                String msgToSend = ReturnStarQuery+msgDelimiter+msgs[1]+contextVariables.getMyNodeId()+msgDelimiter;//ReturnStarQuery####key1<==>value1####...keyn<==>valuen####
                Log.w(TAG,"Return star Query Client Task msg to "+msgs[0]+" :"+msgToSend);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(msgs[0]));
                os= socket.getOutputStream();
                pw= new PrintWriter(os,true);
                pw.println(msgToSend);
                pw.flush();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "Return star Query ClientTask UnknownHostException : "+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Return Star Query ClientTask socket IOException : "+e.getMessage());
            } catch (Exception e){
                Log.e(TAG, " Return star Query ClientTAsk Exception :" + e.getMessage());
            }

            return null;
        }
    }

    private class DeleteStarQueryClientTask extends AsyncTask<Void, Void, Void> {
        @Override
        protected Void doInBackground(Void... msgs) {
            try {
                ContextVariables contextVariables= (ContextVariables)getContext();

                Socket socket;
                OutputStream os;
                PrintWriter pw;
                String msgToSend = StarDelete+msgDelimiter;//StarQuery####originatingPort####
                for(int i=0; i< REMOTE_PORT.length;i++) {
                    if(!REMOTE_PORT[i].equals(contextVariables.getMyPort())) {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT[i]));
                        os = socket.getOutputStream();
                        pw = new PrintWriter(os, true);
                        pw.println(msgToSend);
                        pw.flush();
                        socket.close();
                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "Star Query ClientTask UnknownHostException : "+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Star Query ClientTask socket IOException : "+e.getMessage());
            } catch (Exception e){
                Log.e(TAG, "Star Query ClientTAsk Exception :" + e.getMessage());
            }

            return null;
        }
    }

    private class DeleteClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            try {
                ContextVariables contextVariables= (ContextVariables)getContext();

                Socket socket;
                OutputStream os;
                PrintWriter pw;
                String msgToSend = DeleteTask+msgDelimiter+msgs[1];//DeleteTask####selection####
                Log.w(TAG, "Delete Client Task msg to " + msgs[0] + " :" + msgToSend);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(getPortId(msgs[0])));
                os = socket.getOutputStream();
                pw = new PrintWriter(os, true);
                pw.println(msgToSend);
                pw.flush();
                socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "Delete ClientTask UnknownHostException : "+e.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Delete ClientTask socket IOException : "+e.getMessage());
            } catch (Exception e){
                Log.e(TAG, "Delete ClientTAsk Exception :" + e.getMessage());
            }

            return null;
        }
    }

    private class DeleteReplicaTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            //msgs 0 - dest portId, 1 - key
            ContextVariables contextVariables= (ContextVariables)getContext();
            Socket socket;
            OutputStream os;
            PrintWriter pw;
            String[] preferenceList = new String[]{getPredecessorOne(msgs[0]),getPredecessorTwo(msgs[0])};
            for(String replicatePort: preferenceList){
                try {
                    String msgToSend = DeleteTask+msgDelimiter+msgs[1];//DeleteTask####selection####
                    Log.w(TAG, "Delete Replicate Client Task msg to " + replicatePort + " :" + msgToSend);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(getPortId(replicatePort)));
                    os = socket.getOutputStream();
                    pw = new PrintWriter(os, true);
                    pw.println(msgToSend);
                    pw.flush();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "Delete Replicate ClientTask UnknownHostException : "+e.getMessage());
                } catch (IOException e) {
                    Log.e(TAG, "Delete Replicate ClientTask socket IOException : "+e.getMessage());
                } catch (Exception e){
                    Log.e(TAG, " Delete Replicate ClientTask Exception :" + e.getMessage());
                }
            }
            return null;
        }
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

    private String keyLookUp(String newkey){
        try {
//            String hashNewkey ,nodeHash ,predecessorHash;
//            boolean check;
//            for(int i=0;i<Node_List.length;i++){
              String hashNewkey = genHash(newkey);
//                nodeHash = genHash(Node_List[i]);
//                predecessorHash = genHash(Node_List[((i-1)%5 >= 0)?(i-1)%5:((i-1)%5) + 5]);
//
//                if (hashNewkey.compareTo(predecessorHash) > 0 && hashNewkey.compareTo(nodeHash) <= 0) {
//                    check= true;
//                } else if (hashNewkey.compareTo(predecessorHash) > 0 && predecessorHash.compareTo(nodeHash) > 0) {
//                    check= true;
//                } else if (hashNewkey.compareTo(nodeHash) <= 0 && predecessorHash.compareTo(nodeHash) > 0) {
//                    check= true;
//                } else {
//                    check= false;
//                }
//                if(check){
//                    Log.w(TAG, "Key Lookup value returned "+ Node_List[i] +" : " + hashNewkey);
//                    return Node_List[i];
//                }
//            }

            if(genHash(Node_List[0]).compareTo(hashNewkey) >=0 || genHash(Node_List[4]).compareTo(hashNewkey)<0){
                return Node_List[0];
            }else if(genHash(Node_List[1]).compareTo(hashNewkey)>=0 && genHash(Node_List[0]).compareTo(hashNewkey)<0 ){
                return Node_List[1];
            }else if(genHash(Node_List[2]).compareTo(hashNewkey)>=0 && genHash(Node_List[1]).compareTo(hashNewkey)<0 ){
                return Node_List[2];
            }else if(genHash(Node_List[3]).compareTo(hashNewkey)>=0 && genHash(Node_List[2]).compareTo(hashNewkey)<0 ){
                return Node_List[3];
            }else if(genHash(Node_List[4]).compareTo(hashNewkey)>=0 && genHash(Node_List[3]).compareTo(hashNewkey)<0 ){
                return Node_List[4];
            }

        }catch(Exception e){
            Log.e(TAG,"Key LookUp Error "+ e.getMessage());
        }
        Log.e(TAG, "KeyLookupError Null value returned");
        return null;
    }

    /**
     * buildUri() demonstrates how to build a URI for a ContentProvider.
     *
     * @param scheme
     * @param authority
     * @return the URI
     */
    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private String getPortId(String NodeId){
        switch(NodeId){
            case "5554":
                return "11108";
            case "5556":
                return "11112";
            case "5558":
                return "11116";
            case "5560":
                return "11120";
            case "5562":
                return "11124";
        }
        return "";
    }

    private String getPredecessorOne(String node){
        String predecessorOne;
        if(node.equals("5562")){
            return "5560";
        }else if(node.equals("5556")){
            return "5562";
        } else if(node.equals("5554")){
            return "5556";
        } else if(node.equals("5558")){
            return "5554";
        }else if(node.equals("5560")){
            return "5558";
        }
        return null;
    }

    private String getPredecessorTwo(String node){
        String predecessorOne;
        if(node.equals("5562")){
            return "5558";
        }else if(node.equals("5556")){
            return "5560";
        } else if(node.equals("5554")){
            return "5562";
        } else if(node.equals("5558")){
            return "5556";
        }else if(node.equals("5560")){
            return "5554";
        }
        return null;
    }

}
