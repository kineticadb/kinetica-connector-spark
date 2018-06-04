package com.kinetica.spark.ssl;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509KeyManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class X509KeystoreOverride {

    private static final Logger LOG = LoggerFactory.getLogger(X509KeystoreOverride.class);

    public static KeyManager[] newManagers(String keyStorePath, String password) throws Exception {
        X509KeyManager keyManager = X509KeystoreOverride.newInstance(keyStorePath, password);
        return new KeyManager[] { keyManager };
    }

    public static X509KeyManager newInstance(String keyStorePath, String password) throws Exception {

        File keyStoreFile = new File(keyStorePath);
        if(!keyStoreFile.exists()) {
            throw new Exception("Could not find key store: " + keyStorePath);
        }

        LOG.info("Found P12 keystore: {}", keyStoreFile.getAbsolutePath());

        if(password == null) {
            throw new Exception("Keystore password must be specified.");
        }

        //KeyStore keyStore = KeyStore.getInstance("PKCS12");
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());

        try(FileInputStream myKeys = new FileInputStream(keyStoreFile)) {
            keyStore.load(myKeys, password.toCharArray());
        }

        X509KeyManager keyManager = initKeyManager(keyStore, password);
        return keyManager;
    }

    private static X509KeyManager initKeyManager(KeyStore keyStore, String password) throws Exception {

        KeyManagerFactory keyManagerFactory = KeyManagerFactory
                .getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, password.toCharArray());

        // Get hold of the default trust manager
        X509KeyManager resultKm = null;
        for (KeyManager itemKm : keyManagerFactory.getKeyManagers()) {
            if (itemKm instanceof X509KeyManager) {
                resultKm = (X509KeyManager) itemKm;
                break;
            }
        }

        return resultKm;
    }
}
