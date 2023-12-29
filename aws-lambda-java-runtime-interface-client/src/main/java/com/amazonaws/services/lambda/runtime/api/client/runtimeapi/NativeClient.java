/* Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. */

package com.amazonaws.services.lambda.runtime.api.client.runtimeapi;

import org.crac.Context;
import org.crac.Resource;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.annotation.Native;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * This module defines the native Runtime Interface Client which is responsible for all HTTP
 * interactions with the Runtime API.
 */
class NativeClient {
    static class CheckpointState implements Resource {
        enum State {
            WORKING,
            SYNCING,
            SYNCED,
        };

        State state = State.WORKING;

        private void waitFor(State targetState) {
            while (state != targetState) {
                try {
                    this.wait();
                } catch (InterruptedException interruptedException) {
                }
            }
        }

        @Override
        public synchronized void beforeCheckpoint(Context<? extends Resource> context) throws Exception {
            state = State.SYNCING;
            waitFor(State.SYNCED);
            deinitializeClient();
        }

        @Override
        public synchronized void afterRestore(Context<? extends Resource> context) throws Exception {
            initUserAgent();
            state = State.WORKING;
            this.notifyAll();
        }

        public synchronized void syncPoint() {
            if (state == State.SYNCING) {
                state = State.SYNCED;
                this.notifyAll();
            }
            waitFor(State.WORKING);
        }
    }

    static CheckpointState checkpointState = new CheckpointState();

    private static final String NATIVE_LIB_PATH = "/tmp/.libaws-lambda-jni.so";
    public static final String NATIVE_CLIENT_JNI_PROPERTY = "com.amazonaws.services.lambda.runtime.api.client.runtimeapi.NativeClient.JNI";

    static void init() {
        loadJNILib();
        initUserAgent();
        org.crac.Core.getGlobalContext().register(checkpointState);
    }

    private static void loadJNILib() {
        String jniLib = System.getProperty(NATIVE_CLIENT_JNI_PROPERTY);
        if (jniLib != null) {
            System.load(jniLib);
        } else {
            String[] libsToTry = new String[]{
                    "libaws-lambda-jni.linux-x86_64.so",
                    "libaws-lambda-jni.linux-aarch_64.so",
                    "libaws-lambda-jni.linux_musl-x86_64.so",
                    "libaws-lambda-jni.linux_musl-aarch_64.so"
            };
            unpackAndLoadNativeLibrary(libsToTry);
        }
    }


    /**
     * Unpacks JNI library from the JAR to a temporary location and tries to load it using System.load()
     * Implementation based on AWS CRT
     * (ref. <a href="https://github.com/awslabs/aws-crt-java/blob/0e9c3db8b07258b57c2503cfc47c787ccef10670/src/main/java/software/amazon/awssdk/crt/CRT.java#L106-L134">...</a>)
     *
     * @param libsToTry - array of native libraries to try
     */
    static void unpackAndLoadNativeLibrary(String[] libsToTry) {

        List<String> errorMessages = new ArrayList<>();
        for (String libToTry : libsToTry) {
            try (InputStream inputStream = NativeClient.class.getResourceAsStream(
                    Paths.get("/jni", libToTry).toString())) {
                if (inputStream == null) {
                    throw new FileNotFoundException("Specified file not in the JAR: " + libToTry);
                }
                Files.copy(inputStream, Paths.get(NATIVE_LIB_PATH), StandardCopyOption.REPLACE_EXISTING);
                System.load(NATIVE_LIB_PATH);
                return;
            } catch (UnsatisfiedLinkError | Exception e) {
                errorMessages.add(e.getMessage());
            }
        }

        for (int i = 0; i < libsToTry.length; ++i) {
            System.err.println("Failed to load the native runtime interface client library " + libsToTry[i] +
                    ". Exception: " + errorMessages.get(i));
        }
        System.exit(-1);
    }

    private static void initUserAgent() {
        String userAgent = String.format(
                "aws-lambda-java/%s-%s",
                System.getProperty("java.vendor.version"),
                NativeClient.class.getPackage().getImplementationVersion());

        initializeClient(userAgent.getBytes());

    }

    static native void initializeClient(byte[] userAgent);

    private static native InvocationRequest next();

    static InvocationRequest nextWrapper() {
        return next();
    }

    private static native void postInvocationResponse(byte[] requestId, byte[] response);

    static void postInvocationResponseWrapper(byte[] requestId, byte[] response) {
        postInvocationResponse(requestId, response);
        checkpointState.syncPoint();
    }

    static native void deinitializeClient();
}
