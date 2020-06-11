package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
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
    static final int SERVER_PORT = 10000;
    static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
    Uri providerUri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    HashMap<String, String> AVDHashMap = new HashMap<String, String>();
    ArrayList<String> nodesListHashed = new ArrayList<String>();
    int predecessorPort = 0;
    int current_port = 0;
    int successorPort = 0;
    String hashOfPrevNode = "";
    String hashOfCurrentNode = "";
    String hashOfNextNode = "";

    public int getSelectionType(String s) {
        int i = 0;
        if(s.compareTo("@") == 0) {
            i = 1;
        } else if(s.compareTo("*") == 0) {
            i = 2;
        }
        return i;
    }

    public int checkPredecessorSuccessor(int predecessorPort, int successorPort) {
        int op = 0;
        if(predecessorPort == 0 && successorPort == 0) {
            Log.i(TAG, "Has no predecessor port and no successor port");
            op = 1;
        } else if(predecessorPort == 0 && successorPort != 0) {
            Log.i(TAG, "Has successor port but no predecessor port");
            op = 2;
        } else if(predecessorPort != 0 && successorPort == 0) {
            Log.i(TAG, "Has predecessor port but no successor port");
            op = 3;
        } else if(predecessorPort != 0 && successorPort != 0) {
            Log.i(TAG, "Has predecessor port as well as successor port");
            op = 4;
        }
        return op;
    }

    public int getSelectionLen(String selection) {
        int l = 0;
        if(selection.length() == 1) {
            Log.i(TAG, "1 AVD alive");
            l = 1;
        } else if(selection.length() == 5) {
            Log.i(TAG, "1-5 AVDs alive");
            l = 2;
        } else if(selection.length() > 5 || selection.length() > 6) {
            Log.i(TAG, "all AVDs alive");
            l = 3;
        } else {
            Log.i(TAG, "unknown value");
            l = 4;
        }
        return l;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        /* reference taken from: https://stackoverflow.com/questions/3554722/how-to-delete-internal-storage-file-in-android */
        //code added starts
        String[] cpFiles = new String[] {};
        int i = 0;
        String deleteStr = null;
        String response = null;

        try {
            cpFiles = getContext().getFilesDir().list();
        } catch (NullPointerException e) {
            Log.i(TAG, "NullPointerException: delete(): No file fetched");
            e.printStackTrace();
        } catch (Exception e) {
            Log.i(TAG, "Error fetching files");
            e.printStackTrace();
        }
        deleteStr = "Delete:" + successorPort + ":" + selection;
        int sel = getSelectionType(selection);
        int checkSP = checkPredecessorSuccessor(predecessorPort, successorPort);
        int len = getSelectionLen(selection);

        try {
            if(checkSP == 1) {
                for(i = 0; i < cpFiles.length; i++) {
                    if(getContext().deleteFile(cpFiles[i])) {
                        Log.i(TAG, "delete file successful");
                    } else {
                        Log.i(TAG, "delete file failed");
                    }
                }
            } else if(len == 1) {
                if(sel == 1) {
                    for(i = 0; i < cpFiles.length; i++) {
                        if(getContext().deleteFile(cpFiles[i])) {
                            Log.i(TAG, "delete file successful");
                        } else {
                            Log.i(TAG, "delete file failed");
                        }
                    }
                } else if(sel == 2) {
                    Log.i(TAG, "DeleteStr: " + deleteStr);
                    try {
                        response = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteStr).get();
                        Log.i(TAG, "At global delete, response received: " + response);
                        Log.i(TAG, "response received, now delete all files");
                        for(i = 0; i < cpFiles.length; i++) {
                            if(getContext().deleteFile(cpFiles[i])) {
                                Log.i(TAG, "delete file successful");
                            } else {
                                Log.i(TAG, "delete file failed");
                            }
                        }
                    } catch (InterruptedException e) {
                        Log.e(TAG, "InterruptedException in delete");
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        Log.e(TAG, "ExecutionException in delete");
                        e.printStackTrace();
                    } catch (Exception e) {
                        Log.e(TAG, "Exception in delete");
                        e.printStackTrace();
                    }
                }
            } else if(len == 2) {
                if(selection.substring(0, 1).compareTo("*") == 0) {
                    Log.i(TAG, "delete all files on all AVDs");
                    String sp = String.valueOf(successorPort);
                    if(selection.contains(sp)) {
                        Log.i(TAG, "successor port in list of ports");
                    } else {
                        Log.i(TAG, "not in list, delete files");
                        for(i = 0; i < cpFiles.length; i++) {
                            if(getContext().deleteFile(cpFiles[i])) {
                                Log.i(TAG, "delete file successful");
                            } else {
                                Log.i(TAG, "delete file failed");
                            }
                        }
                    }
                    Log.i(TAG, "delete remaining");
                    for(i = 0; i < cpFiles.length; i++) {
                        if(getContext().deleteFile(cpFiles[i])) {
                            Log.i(TAG, "delete file successful");
                        } else {
                            Log.i(TAG, "delete file failed");
                        }
                    }
                } else {
                    Log.i(TAG, "nothing to delete");
                }
            } else if(len == 3) {
                for(i = 0; i < cpFiles.length; i++) {
                    if(selection.contains(cpFiles[i])) {
                        if(getContext().deleteFile(cpFiles[i])) {
                            Log.i(TAG, "delete file successful");
                        } else {
                            Log.i(TAG, "delete file failed");
                        }
                    }
                }
            } else {
                Log.i(TAG, "DeleteStr: " + deleteStr);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteStr);
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: delete()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Exception: delete()");
            e.printStackTrace();
        }
        //code added ends
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        //code added starts
        String insertStr = null;
        String successorNode = String.valueOf(successorPort * 2);
        String hashOfKey = "";

        try {
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            Log.i(TAG, "key: " + key);
            Log.i(TAG, "value: " + value);
            try {
                hashOfKey = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "NoSuchAlgorithmException");
                e.printStackTrace();
            }
            Log.i(TAG, "Hash of key: " + hashOfKey);
            Log.i(TAG, "Hash of present node: " + hashOfCurrentNode);
            insertStr = "Insert:" + successorPort + ":" + key + ":" + value;
            int cp = checkPredecessorSuccessor(predecessorPort, successorPort);

            if (cp == 1) {
                Log.i(TAG, "Case: Single node, no predecessor port, no successor port, inserting at this port: " + current_port + ", with key: " + key);
                try {
                    FileOutputStream fileOutputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    fileOutputStream.write(value.getBytes());
                    fileOutputStream.close();
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "FileNotFoundException");
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e(TAG, "IOException");
                    e.printStackTrace();
                }
            }
            else if(hashOfPrevNode.compareTo(hashOfKey) < 0 && hashOfKey.compareTo(hashOfCurrentNode) <= 0) {
                Log.i(TAG, "Case: predecessor --> object --> currentNode...so insert at current port: " + current_port + ", with key: " + key);
                try {
                    FileOutputStream fileOutputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    fileOutputStream.write(value.getBytes());
                    fileOutputStream.close();
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "FileNotFoundException");
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e(TAG, "IOException");
                    e.printStackTrace();
                }
            }
            else if((hashOfCurrentNode.compareTo(hashOfPrevNode) < 0 && hashOfCurrentNode.compareTo(hashOfNextNode) < 0) && (hashOfPrevNode.compareTo(hashOfKey) < 0 || hashOfKey.compareTo(hashOfCurrentNode) <= 0)) {
                Log.i(TAG, "Case: predecessor --> object --> currentNode --> successor...so insert at current port: " + current_port + ", with key: " + key);
                try {
                    FileOutputStream fileOutputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    fileOutputStream.write(value.getBytes());
                    fileOutputStream.close();
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "FileNotFoundException");
                    e.printStackTrace();
                } catch (IOException e) {
                    Log.e(TAG, "IOException");
                    e.printStackTrace();
                }
            }
            else {
                Log.i(TAG, "Case: lookup algorithm not successful on currentNode, go to successor node and repeat");
                Log.i(TAG, "Proceed to node: " + successorNode);
                Log.i(TAG, "insertStr here: " + insertStr);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertStr, successorNode);
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: insert()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "error in insert()");
            e.printStackTrace();
        }
        //code added ends
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        //code added starts
        try {
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf(Integer.parseInt(portStr) * 2);

            ServerSocket serverSocket = null;
            int thisPort = 0;
            String hashOfPort = "";
            String onCreateStr = "";

            try {
                serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket");
                e.printStackTrace();
                return false;
            }
            current_port = Integer.parseInt(myPort) / 2;
            Log.i(TAG, "Currently at port: " + current_port);
            onCreateStr = "Join:" + current_port;

            for (int i = 0; i < REMOTE_PORT.length; i++) {
                thisPort = Integer.parseInt(REMOTE_PORT[i]) / 2;
                try {
                    hashOfPort = genHash(String.valueOf(thisPort));
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "NoSuchAlgorithmException: error hashing");
                    e.printStackTrace();
                }
                Log.i(TAG, "Port: " + thisPort + ", genHash on this port: " + hashOfPort);
                if (i == 0) {
                    if (current_port == thisPort) {
                        Log.i(TAG, "add to nodes arraylist");
                        if (nodesListHashed.add(hashOfPort)) {
                            Log.i(TAG, "added successfully");
                        } else {
                            Log.i(TAG, "error while adding");
                        }
                    } else {
                        Log.i(TAG, "onCreateStr: " + onCreateStr);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, onCreateStr);
                    }
                }
                if (AVDHashMap.isEmpty() || !AVDHashMap.containsKey(hashOfPort)) {
                    Log.i(TAG, "entry not in hashmap, so add");
                    AVDHashMap.put(hashOfPort, String.valueOf(thisPort));
                } else {
                    Log.i(TAG, "replacing old value");
                    AVDHashMap.remove(hashOfPort);
                    AVDHashMap.put(hashOfPort, String.valueOf(thisPort));
                }
                if (current_port == thisPort) {
                    hashOfCurrentNode = hashOfPort;
                    Log.i(TAG, "hash of current node: " + hashOfCurrentNode);
                }
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: onCreate()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Exception: onCreate()");
            e.printStackTrace();
        }
        //code added ends
        return false;
    }

    public void getCursor(MatrixCursor cursor, String[] cpFiles, String selection, int cnt) {
        try {
            BufferedReader bufferedReader = null;
            int i = 0;
            if (cnt == 1) {
                for (i = 0; i < cpFiles.length; i++) {
                    try {
                        bufferedReader = new BufferedReader(new InputStreamReader(getContext().openFileInput(cpFiles[i])));
                        String line = bufferedReader.readLine();
                        String[] matrixRow = {cpFiles[i], line};
                        cursor.addRow(matrixRow);
                        bufferedReader.close();
                    } catch (IOException e) {
                        Log.e(TAG, "IOException");
                        e.printStackTrace();
                    } catch (Exception e) {
                        Log.e(TAG, "Exception");
                        e.printStackTrace();
                    }
                }
            } else if (cnt == 2) {
                try {
                    bufferedReader = new BufferedReader(new InputStreamReader(getContext().openFileInput(selection)));
                    String line = bufferedReader.readLine();
                    String[] matrixRow = {selection, line};
                    cursor.addRow(matrixRow);
                    bufferedReader.close();
                } catch (IOException e) {
                    Log.e(TAG, "IOException");
                    e.printStackTrace();
                } catch (Exception e) {
                    Log.e(TAG, "Exception");
                    e.printStackTrace();
                }
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: getCursor()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Exception: getCursor()");
            e.printStackTrace();
        }
    }

    public void processQueryFromClient(MatrixCursor cursor, String msg) {
        try {
            String[] clientMsg = msg.split(",");
            Log.i(TAG, "Message elements fetched:");
            for (int j = 0; j < clientMsg.length; j++) {
                Log.i(TAG, clientMsg[j]);
            }
            for (int j = 0; j < clientMsg.length; j++) {
                clientMsg[j] = clientMsg[j].trim();
                String[] pairs = clientMsg[j].split("#");
                String s1 = "";
                int c1 = pairs[0].compareTo(s1);
                int c2 = pairs[1].compareTo(s1);
                int c3 = checkPredecessorSuccessor(c1, c2);
                if (c3 == 4) {
                    if ((pairs[0].substring(0, 1)).compareTo("{") == 0) {
                        pairs[0] = pairs[0].substring(1);
                    }
                    if ((pairs[1].substring(pairs[1].length() - 1)).compareTo("}") == 0) {
                        pairs[1] = pairs[1].substring(0, 32);
                    }
                    String[] matrixRow = {pairs[0], pairs[1]};
                    cursor.addRow(matrixRow);
                } else {
                    Log.i(TAG, "go to next");
                }
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: processQueryFromClient()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Error: processQueryFromClient");
            e.printStackTrace();
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        //code added starts
        try {
            int i = 0;
            String queryStr = "Query:" + successorPort + ":" + selection;
            String queryFromClient = "";
            String[] cpFiles = new String[]{};
            try {
                cpFiles = getContext().getFilesDir().list();
            } catch (NullPointerException e) {
                Log.i(TAG, "NullPointerException: query(): No file fetched");
                e.printStackTrace();
            } catch (Exception e) {
                Log.i(TAG, "Error fetching files");
                e.printStackTrace();
            }
            MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
            cursor.moveToFirst();
            int sel = getSelectionType(selection);
            int checkSP = checkPredecessorSuccessor(predecessorPort, successorPort);
            int len = getSelectionLen(selection);
            BufferedReader bufferedReader = null;

            if(checkSP == 1) {
                if(len == 1) {
                    for(i = 0; i < cpFiles.length; i++) {
                        try {
                            bufferedReader = new BufferedReader(new InputStreamReader(getContext().openFileInput(cpFiles[i])));
                            String line = bufferedReader.readLine();
                            String[] matrixRow = {cpFiles[i], line};
                            cursor.addRow(matrixRow);
                            bufferedReader.close();
                        } catch (IOException e) {
                            Log.e(TAG, "IOException");
                            e.printStackTrace();
                        }
                    }
                    //getCursor(cursor, cpFiles, selection, 1);
                    return cursor;
                } else if(len > 1) {
                    getCursor(cursor, cpFiles, selection, 2);
                    return cursor;
                }
            } else if(sel == 1) {
                if(len == 1) {
                    for(i = 0; i < cpFiles.length; i++) {
                        try {
                            bufferedReader = new BufferedReader(new InputStreamReader(getContext().openFileInput(cpFiles[i])));
                            String line = bufferedReader.readLine();
                            String[] matrixRow = {cpFiles[i], line};
                            cursor.addRow(matrixRow);
                            bufferedReader.close();
                        } catch (IOException e) {
                            Log.e(TAG, "IOException");
                            e.printStackTrace();
                        }
                    }
                    //getCursor(cursor, cpFiles, selection, 1);
                    return cursor;
                } else if(len > 1) {
                    getCursor(cursor, cpFiles, selection, 2);
                    return cursor;
                }
            } else if(sel == 2) {
                try {
                    Log.i(TAG, "querying on all ports");
                    Log.i(TAG, "query string: " + queryStr);
                    queryFromClient = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryStr).get();
                    Log.i(TAG, "received from client as: " + queryFromClient);
                    processQueryFromClient(cursor, queryFromClient);
                    return cursor;
                } catch (Exception e) {
                    Log.e(TAG, "IOException");
                    e.printStackTrace();
                }
            } else if(len == 2) {
                if(selection.substring(0, 1).compareTo("*") == 0) {
                    try {
                        boolean alpha = selection.contains(String.valueOf(successorPort));
                        if(alpha) {
                            Log.i(TAG, "successor port found");
                        } else {
                            queryFromClient = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryStr).get();
                            Log.i(TAG, "received from client as: " + queryFromClient);
                            processQueryFromClient(cursor, queryFromClient);
                        }
                        for(i = 0; i < cpFiles.length; i++) {
                            try {
                                bufferedReader = new BufferedReader(new InputStreamReader(getContext().openFileInput(cpFiles[i])));
                                String line = bufferedReader.readLine();
                                String[] matrixRow = {cpFiles[i], line};
                                cursor.addRow(matrixRow);
                                bufferedReader.close();
                            } catch (IOException e) {
                                Log.e(TAG, "IOException");
                                e.printStackTrace();
                            }
                        }
                        //getCursor(cursor, cpFiles, selection, 1);
                        return cursor;
                    } catch (Exception e) {
                        Log.e(TAG, "Error");
                        e.printStackTrace();
                    }
                }
            } else {
                if(len == 3) {
                    int y = 0;
                    for(i = 0; i < cpFiles.length; i++) {
                        if(cpFiles[i].contains(selection)) {
                            y = 1;
                        } else {
                            Log.i(TAG, "file name not found in string");
                        }
                    }
                    if(y == 1) {
                        getCursor(cursor, cpFiles, selection, 2);
                        return cursor;
                    } else {
                        Socket s = null;
                        DataOutputStream dataOutputStream = null;
                        DataInputStream dataInputStream = null;
                        int nextNode = successorPort * 2;
                        try {
                            s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), nextNode);
                        } catch (Exception e) {
                            Log.e(TAG, "Error creating socket");
                            e.printStackTrace();
                        }
                        try {
                            dataOutputStream = new DataOutputStream(s.getOutputStream());
                            dataOutputStream.writeUTF(queryStr);
                        } catch (IOException e) {
                            Log.e(TAG, "DataOutputStream: IOException");
                            e.printStackTrace();
                        }
                        String queryStrIn = "";
                        try {
                            dataInputStream = new DataInputStream(s.getInputStream());
                            queryStrIn = dataInputStream.readUTF();
                        } catch (IOException e) {
                            Log.e(TAG, "DataInputStream: IOException");
                            e.printStackTrace();
                        }
                        Log.i(TAG, "Input msg: " + queryStrIn);
                        int end = 66;
                        int start = (end / 2) + 1;
                        String value = queryStrIn.substring(start, end);
                        String[] matrixRow = {selection, value};
                        cursor.addRow(matrixRow);
                        return cursor;
                    }
                }
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: query()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Exception e");
            e.printStackTrace();
        }
        //code added ends
        return null;
    }

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

    public int getOperationType(String str) {
        int op = 0;
        try {
            if(str.compareTo("Delete") == 0) {
                Log.i(TAG, "This is delete function call");
                op = 1;
            } else if(str.compareTo("Insert") == 0) {
                Log.i(TAG, "This is insert function call");
                op = 2;
            } else if(str.compareTo("Join") == 0) {
                Log.i(TAG, "This is onCreate function call");
                op = 3;
            } else if(str.compareTo("Query") == 0) {
                Log.i(TAG, "This is query function call");
                op = 4;
            } else if(str.compareTo("Join_1") == 0) {
                Log.i(TAG, "This is onCreate function call for 1st node");
                op = 5;
            } else if(str.compareTo("Join_P") == 0) {
                Log.i(TAG, "This is onCreate function call for predecessor node");
                op = 6;
            } else if(str.compareTo("Join_S") == 0) {
                Log.i(TAG, "This is onCreate function call for successor node");
                op = 7;
            } else {
                Log.i(TAG, "operation type invalid");
                op = 8;
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: getOperationType()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Exception: getOperationType()");
            e.printStackTrace();
        }
        return op;
    }

    public ArrayList getSortedList(ArrayList arr) {
        try {
            Collections.sort(arr);
            Log.i(TAG, "Elements after sort:");
            Iterator<String> iter = arr.iterator();
            while (iter.hasNext()) {
                Log.i(TAG, iter.next());
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: getSortedList()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Exception: getSortedList()");
            e.printStackTrace();
        }
        return arr;
    }

    public String getPredecessor(int index, int count) {
        String prevNode = null;
        int pos = 0;
        String node = null;
        try {
            if((index > 0 && index < count - 1) || (index == count - 1)) {
                pos = index - 1;
                Log.i(TAG, "get previous location");
            } else if(index == 0) {
                pos = count - 1;
                Log.i(TAG, "get second last location");
            }
            if(!nodesListHashed.isEmpty()) {
                node = nodesListHashed.get(pos);
            } else {
                Log.i(TAG, "no element found in nodesListHashed");
            }
            if(!AVDHashMap.isEmpty()) {
                prevNode = AVDHashMap.get(node);
            } else {
                Log.i(TAG, "no entry found in AVDHashMap");
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: getPredecessor()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Exception: getPredecessor()");
            e.printStackTrace();
        }
        return prevNode;
    }

    public String getSuccessor(int index, int count) {
        String nextNode = null;
        int pos = 0;
        String node = null;
        try {
            if(index > 0 && index < count - 1) {
                pos = index + 1;
            } else if(index == 0) {
                pos = 1;
            }
            if(!nodesListHashed.isEmpty()) {
                node = nodesListHashed.get(pos);
            } else {
                Log.i(TAG, "no element found in nodesListHashed");
            }
            if(!AVDHashMap.isEmpty()) {
                nextNode = AVDHashMap.get(node);
            } else {
                Log.i(TAG, "no entry found in AVDHashMap");
            }
        } catch (NoSuchFieldError e) {
            Log.e(TAG, "NoSuchFieldError: getSuccessor()");
            e.printStackTrace();
        } catch (Exception e) {
            Log.e(TAG, "Exception: getSuccessor()");
            e.printStackTrace();
        }
        return nextNode;
    }

    //code added starts
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            DataInputStream dataIn = null;
            DataOutputStream dataOut = null;
            String inputStr = "";
            HashMap<String, String> keyValueMap = new HashMap<String, String>();
            ServerSocket serverSocket = sockets[0];

            try {
                while(true) {
                    Socket socket = serverSocket.accept();
                    try {
                        dataIn = new DataInputStream(socket.getInputStream());
                        inputStr = dataIn.readUTF();
                    } catch (IOException e) {
                        Log.e(TAG, "DataInputStream: IOException");
                        e.printStackTrace();
                    }
                    Log.i(TAG, "input string read: " + inputStr);
                    try {
                        dataOut = new DataOutputStream(socket.getOutputStream());
                    } catch (IOException e) {
                        Log.e(TAG, "DataOutputStream: IOException");
                        e.printStackTrace();
                    }
                    String[] inMsg = inputStr.split(":");
                    String optype = inMsg[0];
                    int oT = getOperationType(optype);

                    if(oT == 1) {
                        String s1;
                        String typeOps = inMsg[2];
                        if(typeOps.compareTo("*") == 0) {
                            s1 = typeOps + inMsg[1];
                        } else {
                            s1 = inputStr;
                        }
                        Log.i(TAG, "calling delete!");
                        delete(providerUri, s1, null);
                    }
                    else if(oT == 2) {
                        String key = inMsg[2];
                        String value = inMsg[3];
                        ContentValues keyValueToInsert = new ContentValues();
                        Log.i(TAG, "key: " + key);
                        Log.i(TAG, "value: " + value);
                        keyValueToInsert.put("key", key);
                        keyValueToInsert.put("value", value);
                        if(keyValueMap.isEmpty() || !keyValueMap.containsKey(key)) {
                            Log.i(TAG, "entry not found, so insert");
                            keyValueMap.put(key, value);
                        } else {
                            Log.i(TAG, "remove old entry, add new");
                            keyValueMap.remove(key);
                            keyValueMap.put(key, value);
                        }
                        Log.i(TAG, "calling insert!");
                        insert(providerUri, keyValueToInsert);
                    }
                    else if(oT == 3) {
                        String msg = inMsg[1];
                        String hashVal = "";
                        try {
                            hashVal = genHash(msg);
                        } catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "Error getting genHash value");
                            e.printStackTrace();
                        }
                        if(nodesListHashed.add(hashVal)) {
                            Log.i(TAG, "entry added successfully");
                        } else {
                            Log.i(TAG, "error adding entry to nodesListHashed");
                        }
                        nodesListHashed = getSortedList(nodesListHashed);
                        int nodeIndex = 0;
                        int nodeCnt = 0;
                        Iterator<String> iter = nodesListHashed.iterator();
                        while(iter.hasNext()) {
                            if(iter.next().compareTo(hashVal) == 0) {
                                nodeIndex = nodeCnt;
                            }
                            nodeCnt = nodeCnt + 1;
                        }
                        Log.i(TAG, "entry found at index: " + nodeIndex);
                        Log.i(TAG, "total elements on list: " + nodeCnt);
                        String prev = getPredecessor(nodeIndex, nodeCnt);
                        String next = getSuccessor(nodeIndex, nodeCnt);
                        Log.i(TAG, "previous node: " + prev);
                        Log.i(TAG, "next node: " + next);
                        String joinStr1= "Join_1:" + msg + ":" + prev + ":" + next;
                        Log.i(TAG, "sending data to client for 1st node on join");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinStr1);
                        Log.i(TAG, "sending data to client for predecessor node on join");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Join_S:" + prev + ":" + msg);
                        Log.i(TAG, "sending data to client for successor node on join");
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Join_P:" + next + ":" + msg);
                    }
                    else if(oT == 4) {
                        /* reference taken from: https://www.codota.com/code/java/classes/android.database.Cursor ,
                            https://www.codota.com/code/java/methods/android.database.Cursor/moveToNext ,
                            https://examples.javacodegeeks.com/android/core/database/android-cursor-example/ ,
                            https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                        */
                        ArrayList<String> listOfNodes = new ArrayList<String>();
                        HashMap<String, String> hashMapNodes = new HashMap<String, String>();
                        String typeOps = inMsg[2];
                        String selectionString = "";
                        if(typeOps.compareTo("*") == 0) {
                            selectionString = "*" + current_port;
                        } else {
                            selectionString = typeOps;
                        }
                        Cursor cursor = query(providerUri, null, selectionString, null, null);
                        try {
                            if(cursor != null) {
                                String keyToAdd = "";
                                String valueToAdd = "";
                                for(cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                                    keyToAdd = cursor.getString(cursor.getColumnIndex("key"));
                                    valueToAdd = cursor.getString(cursor.getColumnIndex("value"));
                                    if(listOfNodes.add(keyToAdd)) {
                                        Log.i(TAG, "element added successfully");
                                    } else {
                                        Log.i(TAG, "error adding");
                                    }
                                    if(typeOps.compareTo("*") == 0) {
                                        Log.i(TAG, "single entry val");
                                        if(hashMapNodes.isEmpty() || !hashMapNodes.containsKey(keyToAdd)) {
                                            hashMapNodes.put(keyToAdd, valueToAdd);
                                        } else {
                                            hashMapNodes.remove(keyToAdd);
                                            hashMapNodes.put(keyToAdd, valueToAdd);
                                        }
                                    } else {
                                        Log.i(TAG, "multiple entries val");
                                        String strO = "{" + typeOps ;
                                        String value = null;
                                        String[] valArr = valueToAdd.split("#");
                                        for(int i = 0; i < valArr.length; i++) {
                                            if(valArr[i].compareTo(strO) == 0) {
                                                Log.i(TAG, "value not found");
                                            } else {
                                                Log.i(TAG, "value found as: " + valArr[i].substring(0,32));
                                                value = valArr[i].substring(0,32);
                                            }
                                        }
                                        if(hashMapNodes.isEmpty() || !hashMapNodes.containsKey(keyToAdd)) {
                                            hashMapNodes.put(keyToAdd, value);
                                        } else {
                                            hashMapNodes.remove(keyToAdd);
                                            hashMapNodes.put(keyToAdd, value);
                                        }
                                    }
                                }
                                cursor.close();
                            } else {
                                Log.i(TAG, "cursor fetched no rows");
                            }
                        } catch (Exception e) {
                            Log.e(TAG, "Exception at cursor operation");
                            e.printStackTrace();
                        }
                        String strToSend = "{";
                        Iterator<String> iter = listOfNodes.iterator();
                        while(iter.hasNext()) {
                            String key = iter.next();
                            String val = null;
                            if(hashMapNodes.isEmpty() || !hashMapNodes.containsKey(key)) {
                                Log.i(TAG, "no entry found");
                            } else {
                                val = hashMapNodes.get(key);
                                Log.i(TAG, "entry found as: " + val);
                            }
                            strToSend = strToSend + key + "#" + val + ", ";
                        }
                        if(strToSend.substring(strToSend.length()-1).compareTo(",") == 0) {
                            strToSend = strToSend.substring(0, strToSend.length() - 1) + "}";
                        } else {
                            strToSend = strToSend + "}";
                        }
                        Log.i(TAG, "string to be sent: " + strToSend);
                        dataOut.writeUTF(strToSend);
                        dataOut.flush();
                    }
                    else {
                        if(oT == 5 || oT == 6) {
                            predecessorPort =  Integer.parseInt(inMsg[2]);
                            Log.i(TAG, "predecessor: " + predecessorPort);
                            try {
                                hashOfPrevNode = genHash(inMsg[2]);
                            } catch (NoSuchAlgorithmException e) {
                                Log.e(TAG, "NoSuchAlgorithmException: hashOfPrevNode");
                                e.printStackTrace();
                            }
                            Log.i(TAG, "hash of predecessor: " + hashOfPrevNode);
                            if(oT == 5) {
                                successorPort = Integer.parseInt(inMsg[3]);
                                Log.i(TAG, "successor: " + successorPort);
                                try {
                                    hashOfNextNode = genHash(inMsg[3]);
                                } catch (NoSuchAlgorithmException e) {
                                    Log.e(TAG, "NoSuchAlgorithmException: hashOfNextNode");
                                    e.printStackTrace();
                                }
                                Log.i(TAG, "hash of succcessor: " + hashOfNextNode);
                            }
                        }
                        else if(oT == 7) {
                            successorPort = Integer.parseInt(inMsg[2]);
                            Log.i(TAG, "successor: " + successorPort);
                            try {
                                hashOfNextNode = genHash(inMsg[2]);
                            } catch (NoSuchAlgorithmException e) {
                                Log.e(TAG, "NoSuchAlgorithmException: hashOfNextNode");
                                e.printStackTrace();
                            }
                            Log.i(TAG, "hash of succcessor: " + hashOfNextNode);
                        }
                    }
                    dataOut.close();
                    dataIn.close();
                    socket.close();
                }
            } catch (IOException e) {
                Log.e(TAG, "IOException: ServerTask");
                e.printStackTrace();
            } catch (NullPointerException e) {
                Log.e(TAG, "NullPointerException: ServerTask");
                e.printStackTrace();
            } catch (NoSuchFieldError e) {
                Log.e(TAG, "NoSuchFieldError: ServerTask");
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "Exception in ServerTask");
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, String, String> {

        @Override
        protected String doInBackground(String... strings) {
            Socket clientSocket = null;
            Log.i(TAG, "Received string: " + strings[0]);
            String[] inStr = strings[0].split(":");
            String optype = inStr[0];
            int type = getOperationType(optype);
            int destPort = 0;
            DataOutputStream dataOut = null;
            DataInputStream dataIn = null;
            String inMessage = "COMPLETE";
            try {
                if(type == 3) {
                    Log.i(TAG, "operation type is: " + inStr[0]);
                    destPort = Integer.parseInt(REMOTE_PORT[0]);
                } else {
                    Log.i(TAG, "operation type is: " + inStr[0]);
                    destPort = Integer.parseInt(inStr[1]) * 2;
                }
                clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), destPort);
                dataOut = new DataOutputStream(clientSocket.getOutputStream());
                Log.i(TAG, "Msg to write: " + strings[0]);
                dataOut.writeUTF(strings[0]);
                dataOut.flush();
                clientSocket.setSoTimeout(500);
                dataIn = new DataInputStream(clientSocket.getInputStream());
                try {
                    inMessage = dataIn.readUTF();
                } catch (IOException e) {
                    Log.e(TAG, "IOException: ClientTask");
                    e.printStackTrace();
                    inMessage = "ERROR";
                }
                clientSocket.close();
                if(inMessage.compareTo("ERROR") == 0) {
                    Log.i(TAG, "error reading msg to client");
                    Log.e(TAG, "error reading msg to client");
                } else if(inMessage.compareTo("COMPLETE") == 0) {
                    Log.i(TAG, "do nothing!");
                } else {
                    Log.i(TAG, "message received from server as: " + inMessage);
                    return inMessage;
                }
            } catch (IOException e) {
                Log.e(TAG, "IOException: ClientTask");
                e.printStackTrace();
            } catch (NullPointerException e) {
                Log.e(TAG, "NullPointerException: ClientTask");
                e.printStackTrace();
            } catch (NoSuchFieldError e) {
                Log.e(TAG, "NoSuchFieldError: ClientTask");
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "Exception: ClientTask");
                e.printStackTrace();
            }
            return null;
        }
    }
    //code added ends
}