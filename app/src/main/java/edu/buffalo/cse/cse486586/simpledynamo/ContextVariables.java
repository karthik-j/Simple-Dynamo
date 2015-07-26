package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Application;

/**
 * Created by karthikj on 4/29/15.
 */
public class ContextVariables extends Application{
    private static String myPort="";
    private static String myNodeId="";
    private static String myHashNode="";
    private static String mySuccessorOne="";
    private static String mySuccessorTwo="";
//    private static String myHashSuccessor="";
//    private static String myPredecessor="";
//    private static String myHashPredecessor="";

    public static String getMyPort() {
        return myPort;
    }

    public static void setMyPort(String myPort) {
        ContextVariables.myPort = myPort;
    }

    public static String getMyNodeId() {
        return myNodeId;
    }

    public static void setMyNodeId(String myNodeId) {
        ContextVariables.myNodeId = myNodeId;
    }

    public static String getMyHashNode() {
        return myHashNode;
    }

    public static void setMyHashNode(String myHashNode) {
        ContextVariables.myHashNode = myHashNode;
    }

    public static String getMySuccessorOne() {
        return mySuccessorOne;
    }

    public static void setMySuccessorOne(String mySuccessorOne) {
        ContextVariables.mySuccessorOne = mySuccessorOne;
    }

    public static String getMySuccessorTwo() {
        return mySuccessorTwo;
    }

    public static void setMySuccessorTwo(String mySuccessorTwo) {
        ContextVariables.mySuccessorTwo = mySuccessorTwo;
    }

//    public static String getMyHashSuccessor() {
//        return myHashSuccessor;
//    }
//
//    public static void setMyHashSuccessor(String myHashSuccessor) {
//        ContextVariables.myHashSuccessor = myHashSuccessor;
//    }

//    public static String getMyPredecessor() {
//        return myPredecessor;
//    }
//
//    public static void setMyPredecessor(String myPredecessor) {
//        ContextVariables.myPredecessor = myPredecessor;
//    }
//
//    public static String getMyHashPredecessor() {
//        return myHashPredecessor;
//    }
//
//    public static void setMyHashPredecessor(String myHashPredecessor) {
//        ContextVariables.myHashPredecessor = myHashPredecessor;
//    }
}
