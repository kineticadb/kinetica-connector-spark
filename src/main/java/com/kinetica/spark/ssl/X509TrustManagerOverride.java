package com.kinetica.spark.ssl;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class X509TrustManagerOverride implements X509TrustManager {

    private static final Logger LOG = LoggerFactory.getLogger(X509TrustManagerOverride.class);

    public static TrustManager[] newManagers(String trustStorePath, String password) throws Exception {
        X509TrustManager trustManager = X509TrustManagerOverride.newInstance(trustStorePath, password);
        return new TrustManager[] { trustManager };
    }

    public static X509TrustManagerOverride newInstance(String trustStorePath, String password) throws Exception {

        File trustStore = new File(trustStorePath);
        if(!trustStore.exists()) {
            throw new Exception("Could not find trust store: " + trustStorePath);
        }
        LOG.info("Found JKS truststore: {}", trustStore.getAbsolutePath());

        if(password == null) {
            throw new Exception("Truststore password must be specified.");
        }

        // Based on an example from
        // https://stackoverflow.com/questions/24555890/using-a-custom-truststore-in-java-as-well-as-the-default-one

        KeyStore myTrustStore = KeyStore.getInstance(KeyStore.getDefaultType());

        try(FileInputStream myKeys = new FileInputStream(trustStore)) {
            myTrustStore.load(myKeys, password.toCharArray());
        }

        // init the trust manager twice and save the results
        final X509TrustManager defaultTm = initTrustManager(null);
        final X509TrustManager overrideTm = initTrustManager(myTrustStore);

        // create the new trust manager.
        X509TrustManagerOverride finalTm = new X509TrustManagerOverride(defaultTm, overrideTm);

        return finalTm;
    }

    private final X509TrustManager defaultTm;
    private final X509TrustManager overrideTm;

    private X509TrustManagerOverride(X509TrustManager defaultTm, X509TrustManager overrideTm) {
        this.defaultTm = defaultTm;
        this.overrideTm = overrideTm;
    }

    private static X509TrustManager initTrustManager(KeyStore ks) throws Exception {

        // Using null here initialises the TMF with the default trust store.
        TrustManagerFactory tmf = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) ks);

        // Get hold of the default trust manager
        X509TrustManager defaultTm = null;
        for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                defaultTm = (X509TrustManager) tm;
                break;
            }
        }

        return defaultTm;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        // If you're planning to use client-cert auth,
        // do the same as checking the server.
        this.defaultTm.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        try {
            this.overrideTm.checkServerTrusted(chain, authType);
        }
        catch (CertificateException e) {
            // This will throw another CertificateException if this fails too.
            this.defaultTm.checkServerTrusted(chain, authType);
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        // If you're planning to use client-cert auth,
        // merge results from "defaultTm" and "myTm".
        return this.defaultTm.getAcceptedIssuers();
    }
}
