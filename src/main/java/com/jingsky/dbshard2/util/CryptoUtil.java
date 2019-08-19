/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jingsky.dbshard2.util;

import java.security.MessageDigest;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class CryptoUtil {
    public static String generateMd5(Object original) {
        if ((original == null)) {
            return null;
        }
        byte[] defaultBytes = original.toString().getBytes();
        try {
            MessageDigest algorithm = MessageDigest.getInstance("MD5");
            algorithm.reset();
            algorithm.update(defaultBytes);
            byte messageDigest[] = algorithm.digest();

            StringBuffer hexString = new StringBuffer();
            for (int i = 0; i < messageDigest.length; i++) {
                String hex = Integer.toHexString(0xFF & messageDigest[i]);
                if (hex.length() == 0) {
                    hex = "00";
                }
                else if (hex.length() == 1) {
                    hex = "0" + hex;
                }
                hexString.append(hex);
            }
            return hexString.toString();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String generateHmacSHA256(String data, String key) {
    	String result = null;
    	try{
	        SecretKeySpec secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "HmacSHA256");
	        Mac mac = Mac.getInstance("HmacSHA256");
	        mac.init(secretKey);
	        result = new Base64().encode(mac.doFinal(data.getBytes("UTF-8")));
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    	return result;
    }
    
    public static final DesEncrypter getDesEncrypter(String passPhrase){
    	return new DesEncrypter(passPhrase);
    }
    
    public static final class DesEncrypter {
    	private Cipher ecipher;
    	private Cipher dcipher;
        private final byte[] defaultSalt = { (byte) 0xA9, (byte) 0x9B, (byte) 0xC8, (byte) 0x32, (byte) 0x56, (byte) 0x35, (byte) 0xE3, (byte) 0x03 };
        private final int iterations = 13;
        private final Base64 base64 = new Base64();
        public DesEncrypter(String passPhrase){
        	try{
        		KeySpec keySpec = new PBEKeySpec(passPhrase.toCharArray(), defaultSalt, iterations);
                SecretKey key = SecretKeyFactory.getInstance("PBEWithMD5AndDES").generateSecret(keySpec);
                ecipher = Cipher.getInstance(key.getAlgorithm());
                dcipher = Cipher.getInstance(key.getAlgorithm());

                // Prepare the parameter to the ciphers
                AlgorithmParameterSpec paramSpec = new PBEParameterSpec(defaultSalt, iterations);

                // Create the ciphers
                ecipher.init(Cipher.ENCRYPT_MODE, key, paramSpec);
                dcipher.init(Cipher.DECRYPT_MODE, key, paramSpec);
        	}catch(Exception e){
        		e.printStackTrace();
        	}
        }
        public String encrypt(String value){
        	String ret = null;
        	try{
        		byte[] bytes = ecipher.doFinal(value.getBytes("UTF8"));
        		ret = base64.encode(bytes);
        	}catch(Exception e){
        		e.printStackTrace();
        	}
        	return ret;
        }
        public String decrypt(String encryptedString){
        	String ret = null;
        	try{
        		byte[] bytes = base64.decode(encryptedString);
        		ret = new String(dcipher.doFinal(bytes),"UTF8");
        	}catch(Exception e){
        		e.printStackTrace();
        	}
        	return ret;
        }
    }
    
    public static void main(String[] args) throws Exception {
    	DesEncrypter desc = getDesEncrypter("6502247603");
        String str = "john";
        String desEnc = desc.encrypt(str);
        System.out.println(desEnc);
        String desDec = desc.decrypt(desEnc);
        System.out.println(desDec);
    }
}
