/*
 * Copyright 2018 Synesis-Partners.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.system;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Random;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 *
 * @author Federal Highway Administration
 */
public class PasswordUtil
{
	private static final Random RANDOM = new SecureRandom();
	public static final int ITERATIONS = 100000;
	private static final int KEY_LENGTH = 256;
	private final static char[] HEXCHARS = "0123456789abcdef".toCharArray();
	
	public static void main(String[] sArgs) throws Exception
	{
//		try (BufferedWriter oOut = new BufferedWriter(new FileWriter("C:/Users/aaron.cherney/Documents/IMRCP/inserts.txt"));
//		     BufferedReader oIn = new BufferedReader(new FileReader("C:/Users/aaron.cherney/Documents/IMRCP/file.txt")))
//		{
//			String sLine = null;
//			while ((sLine = oIn.readLine()) != null)
//			{
//				String[] sCols = sLine.split(",");
//				String sSalt = PasswordUtil.createSaltString(16);
//				String sEncryptedPw = PasswordUtil.generateSecurePassword(sCols[1], PasswordUtil.getSaltBytes(sSalt));
//				String sEncodedPw = String.format("%s$%d$%s", sSalt, PasswordUtil.ITERATIONS, sEncryptedPw);
//				oOut.write(String.format("INSERT INTO users VALUES (\'%s\', \'%s\');\n", sCols[0], sEncodedPw));
//				oOut.write(String.format("INSERT INTO user_roles VALUES (\'%s\', \'%s\');\n", sCols[0], "imrcp-user"));
//			}
			String sPw = "testpassword123";
			String sSalt = createSaltString(16);
			String sStore = generateSecurePassword(sPw, getSaltBytes(sSalt));
			System.out.println("Password: " + sStore);
			System.out.println("Salt: " + sSalt);
			System.out.println("Length: " + sStore.length());
			System.out.println("Length: " + sSalt.length());
			if (verifyPassword(sPw, sStore, getSaltBytes(sSalt)))
				System.out.println("They match!");
			else
				System.out.println("Access denied");
//		}
	}
	
	public static String createSaltString(int nLength)
	{
		byte[] yBytes = new byte[nLength];
		RANDOM.nextBytes(yBytes);
		return bytesToHex(yBytes);
	}
	
	public static byte[] getSaltBytes(String sSalt)
	{
		byte[] ySaltBytes = new byte[sSalt.length() / 2];
		int nCount = 0;
		for (int i = 0; i < sSalt.length(); i += 2)
			ySaltBytes[nCount++] = (byte) ((byte)(Character.digit(sSalt.charAt(i), 16) << 4) + (byte)Character.digit(sSalt.charAt(i + 1), 16)); // convert the hex digits back to original byte array
		
		return ySaltBytes;
	}	
	
	public static byte[] hash(char[] cPw, byte[] ySalt)
	{
		PBEKeySpec oSpec = new PBEKeySpec(cPw, ySalt, ITERATIONS, KEY_LENGTH);
		Arrays.fill(cPw, Character.MIN_VALUE);
		try
		{
			SecretKeyFactory oSkf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
			return oSkf.generateSecret(oSpec).getEncoded();
		}
		catch (NoSuchAlgorithmException | InvalidKeySpecException oEx)
		{
			throw new AssertionError("Error while hashing a password: " + oEx.getMessage(), oEx);
		}
		finally
		{
			oSpec.clearPassword();
		}
	}
	
	public static String generateSecurePassword(String sPw, byte[] ySalt)
	{
		return bytesToHex(hash(sPw.toCharArray(), ySalt));
	}
	
	public static boolean verifyPassword(String sProvidedPw, String sStoredPw, byte[] ySalt)
	{
		return generateSecurePassword(sProvidedPw, ySalt).equals(sStoredPw);
	}
	
	
	public static String bytesToHex(byte[] yBytes) 
	{
		char[] cHex = new char[yBytes.length * 2];
		for (int i = 0; i < yBytes.length; i++) 
		{
			int v = yBytes[i] & 0xFF;
			cHex[i * 2] = HEXCHARS[v >>> 4];
			cHex[i * 2 + 1] = HEXCHARS[v & 0x0F];
		}
		return new String(cHex);
	}	
}
