/*
 * Copyright (C) 2016 phantombot.tv
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.gmt2001;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.thegreshams.firebase4j.error.FirebaseException;
import net.thegreshams.firebase4j.error.JacksonUtilityException;
import net.thegreshams.firebase4j.model.FirebaseResponse;
import net.thegreshams.firebase4j.service.Firebase;

/**
 * 
 * @author crehbein 
 */
public class FirebaseStore extends DataStore {

	// Hardcoded, for now
	private static final String testBaseUrl = "https://phantombot-firebase.firebaseio.com/";
	private static final String testAuthKey = "t8wqw7WNGpYA2Y5WqG5asgv6qT25JuHuHXfpeVCn";
	
	private static final String dbPrefix = "phantombot_";
	
	private static final String DOT = "DOT";
	private static final String SLASH = "SLASH";
	private static final String SPACE = "SPACE";
	private static final String EXT = "JSEXTENSION";
	
	private Firebase firebase;
	
	private static FirebaseStore instance;
	
	public static FirebaseStore instance(String oauth, String path) {
		
		if (instance == null) {
			instance = new FirebaseStore(oauth, path);
		}
		return instance;
	}
	
	private FirebaseStore(String oauth, String path) {
		try {
			firebase = new Firebase(path, oauth);
		} catch (FirebaseException e) {
			e.printStackTrace();
			com.gmt2001.Console.err.printStackTrace(e);
		}
	}
	
	public static final void main(String[] args) {
		
		FirebaseStore store = FirebaseStore.instance(testAuthKey, testBaseUrl);
		
		store.AddFile("tablename");
		store.SetString("tablename", "", "ctnp_twitch", "2");
		String s = store.GetString("tablename", "", "ctnp_twitch");
		
		store.GetKeyList("modules", "");
		
		System.out.println("Value of s: " + s);
		
		String[] tables = store.GetFileList();
		for (String str : tables) {
			System.out.println(str);
		}
		
	}
    
    @Override
    public Object CreateConnection(String db, String username, String password) {
    	try {
    		FirebaseResponse response = firebase.get("/");
    		
    		if (response.getSuccess()) {
    			return new Integer(0);
    		} else {
    			return null;
    		}
        } catch (Exception ex) {
        	com.gmt2001.Console.err.printStackTrace(ex);        	
        } catch (FirebaseException fe) {
        	com.gmt2001.Console.err.printStackTrace(fe);        	
        }
    	return null;    	
    }	
	
    @Override
    public boolean FileExists(String fName) {

        fName = validateFname(fName);

        try {
        	
    		String path = dbPrefix + fName;
    		path = normalizePath(path);
    		
        	FirebaseResponse response  = firebase.get(path);
        	
        	if (!response.getSuccess()) {
        		throw new FirebaseException("Endpoint responded with error: " + response.getCode() + " - Request URL was: " + path);
        	} else {
        		if (response.getBody().size() > 0) {
        			return true;
        		}         		
        	}
        	        	
        } catch (Exception ex) {
        	com.gmt2001.Console.err.printStackTrace(ex);        	
        } catch (FirebaseException fe) {
        	com.gmt2001.Console.err.printStackTrace(fe);        	
        }
        
        return false;
    }
    
    @Override
    public String[] GetFileList() {
    	    	
    	try {    		    		  		    
    		
    		FirebaseResponse response = firebase.get("/");
    
    		if (response.getSuccess()) {
    			
    			String[] keysList = response.getBody().keySet().toArray(new String[response.getBody().keySet().size()]);
    			ArrayList<String> chopped = new ArrayList<String>();
    			
    			for (String s : keysList) {
    				chopped.add(s.substring(dbPrefix.length()));
    			}
    			    			
    			return chopped.toArray(new String[chopped.size()]);
    			
    		} else {
    			throw new FirebaseException("Endpoint responded with error: " + response.getCode() + " - Request URL was the root path.");
    		}
    		    		    		
        } catch (Exception ex) {
        	com.gmt2001.Console.err.printStackTrace(ex);        	
        } catch (FirebaseException fe) {
        	com.gmt2001.Console.err.printStackTrace(fe);        	
        }    	    	    	
    	
    	return new String[0];
    }
        
    @Override
    public void RemoveFile(String filename) {
    	
    	if (FileExists(filename)) {
    		
    		try {
    		
    			String path = dbPrefix + filename;
    			path = normalizePath(path);
    			
    			FirebaseResponse response = firebase.delete(path);
    			
    			if (!response.getSuccess()) {
    				throw new FirebaseException("Endpoint responded with error: " + response.getCode() + " - Request URL was: " + path);
    			}
    		
    		} catch (Exception e) {
    			com.gmt2001.Console.err.printStackTrace(e);
    		} catch (FirebaseException fe) {
    			com.gmt2001.Console.err.printStackTrace(fe);
    		}    		
    	}    	    
    }
    
    @Override
    public String[] GetKeyList(String fName, String section) {
    	
    	fName = validateFname(fName);
    	
    	if (!FileExists(fName)) {
    		return new String[0];
    	}
    	
    	try {
    		
    		String path = dbPrefix + fName;
    		path = normalizePath(path);
    		
    		FirebaseResponse response = firebase.get(path);
    		
    		if (!response.getSuccess()) {
        		throw new FirebaseException("Endpoint responded with error: " + response.getCode() + " - Request URL was: " + path);
    		}
    		
    		Map<String, Object> respMap = response.getBody();
    		
    		Set<String> keys = respMap.keySet();
    		String[] keysStr = keys.toArray(new String[keys.size()]);
    		
    		List<String> retList = new ArrayList<String>();    		
    		
    		for (String k : keysStr) {
    			retList.add(denormalizeKey(k));
    		}
    		
    		String[] retArr = retList.toArray(new String[retList.size()]);
    		
    		return retArr;
    		
    	} catch (FirebaseException e) {
    		e.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(e);    		    		
    	} catch (Exception ex) {    		
    		ex.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(ex);    		    		
		}
    	
    	return new String[0];
    }
        
    @Override
    public boolean HasKey(String fName, String section, String key) {
    	
    	fName = validateFname(fName);
    	
    	if (!FileExists(fName)) {
    		return false;
    	}    	
    	
    	String s = GetString(fName, section, key);
    	
    	if (s != null) {
    		return true;
    	}
    	
    	return false;
    }
    
    @Override
    public String GetString(String fName, String section, String key) {    	
    	
    	try {
    		
    		fName = validateFname(fName);
    		
    		key = normalizeKey(key);
    		
    		String path = dbPrefix + fName + "/" + key;
    		path = normalizePath(path);
    		
    		FirebaseResponse response = firebase.get(path);
    		
        	if (!response.getSuccess()) {
        		throw new FirebaseException("Endpoint responded with error: " + response.getCode() + " - Request URL was: " + path);
        	}
    		
    		String value = (String)response.getBody().get(key);
    		
    		if ("NaN".equalsIgnoreCase(value)) {
    			return "0";
    		}
    		
    		return value;
    		
    	} catch (FirebaseException e) {
    		e.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(e);    		    		
    	} catch (Exception ex) {    		
    		ex.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(ex);    		    		
		}
    	
    	return "";    	
    }
    
    @Override
    public void SetString(String fName, String section, String key, String value) {
    	
    	try {
    		
    		fName = validateFname(fName);
    		
    		key = normalizeKey(key);
    		
    		String path = dbPrefix + fName + "/" + key;
    		
    		Map<String, Object> map = new LinkedHashMap<String, Object>();
    		map.put(key, value);    		
    		
    		FirebaseResponse response = firebase.put(path, map);

        	if (!response.getSuccess()) {
        		throw new FirebaseException("Endpoint responded with error: " + response.getCode() + " - Request URL was: " + path);
        	} 
    		    		
    	} catch (FirebaseException e) {
    		e.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(e);    		    		
    	} catch (Exception ex) {    		
    		ex.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(ex);    		    		
    	} catch (JacksonUtilityException e) {
    		e.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(e);    		    		
		}
    }

	@Override
	public void SetBatchString(String fName, String section, String[] keys, String[] values) {
		
		fName = validateFname(fName);
		
		try {
			
			Map<String, Object> map = new LinkedHashMap<String, Object>();
			
			for (int i = 0; i < keys.length; i++) {
				
				String key = keys[i];
				
				key = normalizeKey(key);
	    		
				map.put(key, values[i]);
				
				String path = dbPrefix + fName + "/" + key;
				//path = path.replace(" ", "_");				
				path = normalizePath(path);
				
				FirebaseResponse response = firebase.put(path, map);
				
	        	if (!response.getSuccess()) {
	        		throw new FirebaseException("Endpoint responded with error: " + response.getCode() + " - Request URL was: " + path);
	        	}
			}
						
    	} catch (FirebaseException e) {
    		e.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(e);    		    		
    	} catch (Exception ex) {    		
    		ex.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(ex);
    	} catch (JacksonUtilityException e) {
    		e.printStackTrace();
    		com.gmt2001.Console.err.printStackTrace(e);    		    		
		}
		
	}
	
    private String validateFname(String fName) {
        fName = fName.replaceAll("([^a-zA-Z0-9_-])", "_");

        return fName;
    }
    
    private String normalizePath(String path) throws Exception {
		path = path.replace(" ", SPACE)
				.replace(".js", EXT)
				.replace(".", DOT);
		
				
		return path;
    }
    
    private String denormalizePath(String path) {
		path = path.replace(SPACE, " ")
				.replace(EXT, "js")
				.replace(DOT, ".");
		
		return path;    	
    }
    
    private String normalizeKey(String key) {
		key = key.replace(" ", SPACE)
			.replace(".js", EXT)
			.replace(".", DOT)			
			.replace("/", SLASH);
    	
		return key;
    }
    
    private String denormalizeKey(String key) {
    	key = key.replace(SPACE, " ")
    			.replace(EXT, ".js")
    			.replace(DOT, ".")    			
    			.replace(SLASH, "/");
    	
    	return key;
    }	
	
}
